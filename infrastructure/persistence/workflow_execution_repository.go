package persistence

import (
	"context"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (s *PostgresWorkflowExecutionRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE EXTENSION IF NOT EXISTS "pgcrypto";

		CREATE TABLE IF NOT EXISTS workflow_execution (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			workflow_definition_id UUID NOT NULL,

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
						'FAILED'
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
	execution *workflow.WorkflowExecution,
) error {
	return s.db.QueryRowContext(
		ctx,
		`
		INSERT INTO workflow_execution (
			workflow_definition_id,
			input,
			state,
			current_step,
			scheduled_at
		)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING
			id,
			created_at,
			updated_at
		`,
		execution.WorkflowDefinitionID,
		execution.Input,
		execution.State,
		execution.CurrentStep,
		execution.ScheduledAt,
	).Scan(
		&execution.ID,
		&execution.CreatedAt,
		&execution.UpdatedAt,
	)
}