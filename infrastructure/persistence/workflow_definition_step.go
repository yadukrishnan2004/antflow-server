package persistence

import (
	"context"
	"database/sql"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (s *PostgresWorkflowDefinitionStepRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE EXTENSION IF NOT EXISTS "pgcrypto";

		CREATE TABLE IF NOT EXISTS workflow_definition_step (
			id                     UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
			workflow_definition_id UUID    NOT NULL,
			step_index             INTEGER NOT NULL,
			step_name              TEXT    NOT NULL,
			compensation_step_name TEXT,
			task_queue             TEXT,
			timeout_seconds        INTEGER NOT NULL DEFAULT 300,
			max_attempts           INTEGER NOT NULL DEFAULT 3,

			CONSTRAINT fk_workflow_definition_step_definition
				FOREIGN KEY (workflow_definition_id)
				REFERENCES workflow_definition(id)
				ON DELETE CASCADE,

			CONSTRAINT uq_workflow_definition_step_order
				UNIQUE (workflow_definition_id, step_index)
		);

		ALTER TABLE workflow_definition_step ADD COLUMN IF NOT EXISTS compensation_step_name TEXT;
		ALTER TABLE workflow_definition_step ADD COLUMN IF NOT EXISTS timeout_seconds INTEGER NOT NULL DEFAULT 300;
		ALTER TABLE workflow_definition_step ADD COLUMN IF NOT EXISTS max_attempts INTEGER NOT NULL DEFAULT 3;
	`)
	return err
}

func (s *PostgresWorkflowDefinitionStepRepository) Create(
	ctx context.Context, step *workflow.WorkflowDefinitionStep,
) error {
	return getDB(ctx,s.db).QueryRowContext(ctx, `
		INSERT INTO workflow_definition_step
			(workflow_definition_id, step_index, step_name, compensation_step_name, task_queue, timeout_seconds,max_attempts)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id
	`,
		step.WorkflowDefinitionID,
		step.StepIndex,
		step.StepName,
		step.CompensationStepName,
		step.TaskQueue,
		step.TimeoutSeconds,
		step.MaxAttempts,
	).Scan(&step.ID)
}

func (s *PostgresWorkflowDefinitionStepRepository) GetStepsByDefinitionID(
	ctx context.Context, definitionID string,
) ([]workflow.WorkflowDefinitionStep, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, workflow_definition_id, step_index, step_name,
		       COALESCE(compensation_step_name, ''), COALESCE(task_queue, ''), timeout_seconds, max_attempts
		FROM   workflow_definition_step
		WHERE  workflow_definition_id = $1
		ORDER  BY step_index ASC
	`, definitionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var steps []workflow.WorkflowDefinitionStep
	for rows.Next() {
		var s workflow.WorkflowDefinitionStep
		if err := rows.Scan(
			&s.ID, &s.WorkflowDefinitionID, &s.StepIndex,
			&s.StepName, &s.CompensationStepName, &s.TaskQueue, &s.TimeoutSeconds, &s.MaxAttempts,
		); err != nil {
			return nil, err
		}
		steps = append(steps, s)
	}
	return steps, rows.Err()
}

func (s *PostgresWorkflowDefinitionStepRepository) GetByDefinitionAndIndex(
	ctx context.Context, definitionID string, stepIndex int,
) (*workflow.WorkflowDefinitionStep, error) {
	step := &workflow.WorkflowDefinitionStep{}
	err := getDB(ctx, s.db).QueryRowContext(ctx, `
		SELECT id, workflow_definition_id, step_index, step_name,
		       COALESCE(compensation_step_name, ''), COALESCE(task_queue, ''), timeout_seconds, max_attempts
		FROM   workflow_definition_step
		WHERE  workflow_definition_id = $1 AND step_index = $2
	`, definitionID, stepIndex).Scan(
		&step.ID, &step.WorkflowDefinitionID, &step.StepIndex,
		&step.StepName, &step.CompensationStepName, &step.TaskQueue, &step.TimeoutSeconds, 
		&step.MaxAttempts,
	)
	if err == sql.ErrNoRows {
		return nil, workflow.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return step, nil
}

func (s *PostgresWorkflowDefinitionStepRepository) GetCompensationSteps(
	ctx context.Context, definitionID string, upToStepIndex int,
) ([]workflow.WorkflowDefinitionStep, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, workflow_definition_id, step_index, step_name,
		       COALESCE(compensation_step_name, ''), COALESCE(task_queue, ''), timeout_seconds,  max_attempts
		FROM   workflow_definition_step
		WHERE  workflow_definition_id = $1 
		  AND  step_index <= $2
		  AND  compensation_step_name IS NOT NULL 
		  AND  compensation_step_name <> ''
		ORDER  BY step_index DESC
	`, definitionID, upToStepIndex)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var steps []workflow.WorkflowDefinitionStep
	for rows.Next() {
		var s workflow.WorkflowDefinitionStep
		if err := rows.Scan(
			&s.ID, &s.WorkflowDefinitionID, &s.StepIndex,
			&s.StepName, &s.CompensationStepName, &s.TaskQueue, &s.TimeoutSeconds, &s.MaxAttempts,
		); err != nil {
			return nil, err
		}
		steps = append(steps, s)
	}
	return steps, rows.Err()
}

var _ workflow.WorkflowDefinitionStepRepository = (*PostgresWorkflowDefinitionStepRepository)(nil)