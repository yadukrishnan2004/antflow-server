package persistence

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (s *PostgresWorkflowRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS workflow_definition (
			name          TEXT PRIMARY KEY,
			workflow_type TEXT NOT NULL DEFAULT 'CHAIN',
			description   TEXT,
			created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);

		CREATE TABLE IF NOT EXISTS workflow_definition_step (
			workflow_name   TEXT NOT NULL,
			step_index      INT NOT NULL,
			step_name       TEXT NOT NULL,
			task_queue      TEXT NOT NULL DEFAULT 'default',
			timeout_seconds INT NOT NULL DEFAULT 300,
			PRIMARY KEY (workflow_name, step_index),
			UNIQUE (workflow_name, step_name),
			FOREIGN KEY (workflow_name) REFERENCES workflow_definition(name) ON DELETE CASCADE
		);

		CREATE TABLE IF NOT EXISTS workflow_execution (
			id                 TEXT PRIMARY KEY,
			workflow_name      TEXT NOT NULL,
			workflow_type      TEXT NOT NULL DEFAULT 'CHAIN',
			task_queue         TEXT NOT NULL DEFAULT 'default',
			input              BYTEA,
			result             BYTEA,
			state              TEXT NOT NULL DEFAULT 'CREATED',
			error              TEXT,
			current_step_index INT NOT NULL DEFAULT 0,
			total_steps        INT NOT NULL,
			created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			CONSTRAINT fk_def
				FOREIGN KEY (workflow_name)
				REFERENCES workflow_definition(name)
				ON DELETE RESTRICT
		);
	`)
	return err
}

func (s *PostgresWorkflowRepository) SaveDefinition(def *workflow.WorkflowDefinition) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT INTO workflow_definition (name, workflow_type, description, created_at)
		VALUES ($1, $2, $3, $4)
	`, def.Name, string(def.WorkflowType), def.Description, def.CreatedAt)
	if err != nil {
		return fmt.Errorf("repo: failed to save workflow definition: %w", err)
	}

	for _, step := range def.Steps {
		_, err = tx.Exec(`
			INSERT INTO workflow_definition_step (workflow_name, step_index, step_name, task_queue, timeout_seconds)
			VALUES ($1, $2, $3, $4, $5)
		`, step.WorkflowName, step.StepIndex, step.StepName, step.TaskQueue, step.TimeoutSeconds)
		if err != nil {
			return fmt.Errorf("repo: failed to save step '%s': %w", step.StepName, err)
		}
	}

	return tx.Commit()
}

func (s *PostgresWorkflowRepository) FindDefinitionByName(name string) (*workflow.WorkflowDefinition, error) {
	row := s.db.QueryRow(`
		SELECT name, workflow_type, description, created_at
		FROM workflow_definition
		WHERE name = $1
	`, name)

	var d workflow.WorkflowDefinition
	var workflowTypeStr string
	var desc sql.NullString
	if err := row.Scan(
		&d.Name,
		&workflowTypeStr,
		&desc,
		&d.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: workflow definition not found", workflow.ErrNotFound)
		}
		return nil, fmt.Errorf("repo: failed to find definition: %w", err)
	}
	d.WorkflowType = workflow.WorkflowType(workflowTypeStr)
	if desc.Valid {
		d.Description = desc.String
	}

	rows, err := s.db.Query(`
		SELECT workflow_name, step_index, step_name, task_queue, timeout_seconds
		FROM workflow_definition_step
		WHERE workflow_name = $1
		ORDER BY step_index ASC
	`, name)
	if err != nil {
		return nil, fmt.Errorf("repo: failed to find steps: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var step workflow.WorkflowDefinitionStep
		if err := rows.Scan(&step.WorkflowName, &step.StepIndex, &step.StepName, &step.TaskQueue, &step.TimeoutSeconds); err != nil {
			return nil, err
		}
		d.Steps = append(d.Steps, step)
	}

	return &d, nil
}

func (s *PostgresWorkflowRepository) SaveExecution(exec *workflow.WorkflowExecution) error {
	_, err := s.db.Exec(`
		INSERT INTO workflow_execution (id, workflow_name, workflow_type, task_queue, input, result, state, error, current_step_index, total_steps, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`,
		exec.ID,
		exec.WorkflowName,
		string(exec.WorkflowType),
		exec.TaskQueue,
		nullableBytes(exec.Input),
		nullableBytes(exec.Result),
		string(exec.State),
		nullableString(exec.Error),
		exec.CurrentStepIndex,
		exec.TotalSteps,
		exec.CreatedAt,
		exec.UpdatedAt,
	)

	if err != nil {
		return fmt.Errorf("repo: failed to save workflow execution: %w", err)
	}

	return nil
}

func (s *PostgresWorkflowRepository) FindExecutionByID(id string) (*workflow.WorkflowExecution, error) {
	row := s.db.QueryRow(`
		SELECT id, workflow_name, workflow_type, task_queue, input, result, state, error, current_step_index, total_steps, created_at, updated_at
		FROM workflow_execution
		WHERE id = $1
	`, id)

	var w workflow.WorkflowExecution
	var state string
	var workflowTypeStr string
	var input, result []byte
	var execErr sql.NullString

	if err := row.Scan(
		&w.ID,
		&w.WorkflowName,
		&workflowTypeStr,
		&w.TaskQueue,
		&input,
		&result,
		&state,
		&execErr,
		&w.CurrentStepIndex,
		&w.TotalSteps,
		&w.CreatedAt,
		&w.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("repo: workflow execution not found")
		}
		return nil, fmt.Errorf("repo: failed to find execution: %w", err)
	}

	w.Input = input
	w.Result = result
	w.State = workflow.State(state)
	w.WorkflowType = workflow.WorkflowType(workflowTypeStr)
	if execErr.Valid {
		w.Error = execErr.String
	}

	return &w, nil
}

func (s *PostgresWorkflowRepository) UpdateExecutionState(id string, state workflow.State) error {
	_, err := s.db.Exec(`
		UPDATE workflow_execution
		SET state = $1, updated_at = NOW()
		WHERE id = $2
	`, string(state), id)

	if err != nil {
		return fmt.Errorf("repo: failed to update execution state: %w", err)
	}

	return nil
}

func (s *PostgresWorkflowRepository) FindStep(workflowName string, stepIndex int) (*workflow.WorkflowDefinitionStep, error) {
	row := s.db.QueryRow(`
		SELECT workflow_name, step_index, step_name, task_queue, timeout_seconds
		FROM workflow_definition_step
		WHERE workflow_name = $1 AND step_index = $2
	`, workflowName, stepIndex)

	var step workflow.WorkflowDefinitionStep

	if err := row.Scan(
		&step.WorkflowName, &step.StepIndex, &step.StepName,
		&step.TaskQueue, &step.TimeoutSeconds,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("repo: step %d not found for workflow %s", stepIndex, workflowName)
		}
		return nil, fmt.Errorf("repo: failed to find step: %w", err)
	}
	return &step, nil
}

func (s *PostgresWorkflowRepository) UpdateStepCursor(id string, stepIndex int) error {
	_, err := s.db.Exec(`
        UPDATE workflow_execution
        SET current_step_index = $1, updated_at = NOW()
        WHERE id = $2
    `, stepIndex, id)
    if err != nil {
        return fmt.Errorf("repo: failed to update step cursor: %w", err)
    }
    return nil
}

func (s *PostgresWorkflowRepository) SaveResult(id string, result []byte) error {
	_ , err := s.db.Exec(`
		UPDATE workflow_execution
		SET result = $1 , updated_at = NOW()
		WHERE id = $2	
	`, nullableBytes(result),id)

    if err != nil {
        return fmt.Errorf("repo: failed to save execution result: %w", err)
    }
    return nil
}

func (s *PostgresWorkflowRepository) GetWorkflowNameByExecutionID(executionID string) (string, error) {
	var name string
	err := s.db.QueryRow(`
		SELECT workflow_name FROM workflow_execution WHERE id = $1
	`, executionID).Scan(&name)
	if err != nil {
		return "", fmt.Errorf("repo: failed to get workflow name: %w", err)
	}
	return name, nil
}
