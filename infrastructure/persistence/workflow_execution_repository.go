package persistence

import (
	"context"
	"database/sql"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (s *PostgresWorkflowExecutionRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE EXTENSION IF NOT EXISTS "pgcrypto";

		CREATE TABLE IF NOT EXISTS workflow_execution (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			workflow_definition_id UUID NOT NULL,
			workflow_name TEXT NOT NULL,
			task_queue TEXT NOT NULL DEFAULT 'default',
			total_steps INTEGER NOT NULL,
			workflow_type TEXT NOT NULL,

			input BYTEA,
			result BYTEA,

			state TEXT NOT NULL DEFAULT 'CREATED',
			error TEXT,

			current_step INTEGER NOT NULL DEFAULT 0,

			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			completed_at TIMESTAMPTZ,

			CONSTRAINT fk_workflow_execution_definition
				FOREIGN KEY (workflow_definition_id)
				REFERENCES workflow_definition(id)
				ON DELETE CASCADE,

			CONSTRAINT chk_workflow_execution_state
				CHECK (
					state IN (
						'CREATED',
						'RUNNING',
						'COMPLETED',
						'FAILED',
						'CANCELLED'
					)
				),

			CONSTRAINT chk_workflow_execution_current_step
				CHECK (current_step >= 0)
		);
	`)
	return err
}

func (s *PostgresWorkflowExecutionRepository) Create(
	ctx context.Context,
	exec *workflow.WorkflowExecution,
) error {
	return s.db.QueryRowContext(
		ctx,
		`
		INSERT INTO workflow_execution (
			id,
			workflow_definition_id,
			workflow_name,
			task_queue,
			total_steps,
			workflow_type,
			input,
			state,
			current_step,
			scheduled_at,
			created_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		RETURNING
			created_at,
			updated_at
		`,
		exec.ID,
		exec.WorkflowDefinitionID,
		exec.WorkflowName,
		exec.TaskQueue,
		exec.TotalSteps,
		string(exec.WorkflowType),
		exec.Input,
		string(exec.State),
		exec.CurrentStep,
		exec.ScheduledAt,
		exec.CreatedAt,
		exec.UpdatedAt,
	).Scan(
		&exec.CreatedAt,
		&exec.UpdatedAt,
	)
}

func (s *PostgresWorkflowExecutionRepository) GetByID(
	ctx context.Context,
	id string,
) (*workflow.WorkflowExecution, error) {
	exec := &workflow.WorkflowExecution{}
	var completedAt sql.NullTime
	var stateStr string
	var wfTypeStr string

	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, workflow_definition_id, input, result,
		        state, COALESCE(error, ''), current_step, created_at,
		        scheduled_at, updated_at, completed_at,
		        workflow_name, workflow_type, total_steps,
		        task_queue
		 FROM workflow_execution
		 WHERE id = $1`,
		id,
	).Scan(
		&exec.ID,
		&exec.WorkflowDefinitionID,
		&exec.Input,
		&exec.Result,
		&stateStr,
		&exec.Error,
		&exec.CurrentStep,
		&exec.CreatedAt,
		&exec.ScheduledAt,
		&exec.UpdatedAt,
		&completedAt,
		&exec.WorkflowName,
		&wfTypeStr,
		&exec.TotalSteps,
		&exec.TaskQueue,
	)
	if err == sql.ErrNoRows {
		return nil, workflow.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	exec.WorkflowType = workflow.WorkflowType(wfTypeStr)
	exec.State = workflow.State(stateStr)
	if completedAt.Valid {
		exec.CompletedAt = &completedAt.Time
	}

	return exec, nil
}

func (s *PostgresWorkflowExecutionRepository) UpdateState(
	ctx context.Context,
	id string,
	state workflow.State,
) error {
	var err error
	if state == workflow.StateFailed || state == workflow.StateCancelled {
		_, err = s.db.ExecContext(ctx,
			`UPDATE workflow_execution SET state=$1, completed_at=NOW(), updated_at=NOW() WHERE id=$2`,
			string(state), id)
	} else {
		_, err = s.db.ExecContext(ctx,
			`UPDATE workflow_execution SET state=$1, updated_at=NOW() WHERE id=$2`,
			string(state), id)
	}
	return err
}

func (s *PostgresWorkflowExecutionRepository) UpdateStepCursor(
	ctx context.Context,
	id string,
	nextStep int,
) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE workflow_execution SET current_step=$1, updated_at=NOW() WHERE id=$2`,
		nextStep, id)
	return err
}

func (s *PostgresWorkflowExecutionRepository) SaveResult(
	ctx context.Context,
	id string,
	result []byte,
) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE workflow_execution SET result=$1, completed_at=NOW(), updated_at=NOW() WHERE id=$2`,
		result, id)
	return err
}
