package persistence

import (
	"context"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (s *PostgresTaskRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE EXTENSION IF NOT EXISTS "pgcrypto";

		CREATE TABLE IF NOT EXISTS task (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

			workflow_execution_id UUID NOT NULL,

			step_index INTEGER NOT NULL,
			step_name TEXT NOT NULL,

			input BYTEA NOT NULL,
			output BYTEA,

			state TEXT NOT NULL DEFAULT 'CREATED',
			error TEXT,

			scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			started_at TIMESTAMPTZ,
			completed_at TIMESTAMPTZ,

			attempt INTEGER NOT NULL DEFAULT 1,
			max_attempts INTEGER NOT NULL DEFAULT 3,

			CONSTRAINT fk_task_workflow_execution
				FOREIGN KEY (workflow_execution_id)
				REFERENCES workflow_execution(id)
				ON DELETE CASCADE,

			CONSTRAINT uq_task_workflow_step
				UNIQUE (workflow_execution_id, step_index),

			CONSTRAINT chk_task_state
				CHECK (
					state IN (
						'CREATED',
						'SCHEDULED',
						'RUNNING',
						'COMPLETED',
						'FAILED'
					)
				),

			CONSTRAINT chk_task_attempt
				CHECK (attempt > 0),

			CONSTRAINT chk_task_max_attempts
				CHECK (max_attempts > 0)
		);

		CREATE INDEX IF NOT EXISTS idx_task_state_scheduled_at
			ON task (state, scheduled_at);
	`)
	return err
}

func (s *PostgresTaskRepository) Create(
	ctx context.Context,
	task *workflow.Task,
) error {
	return s.db.QueryRowContext(
		ctx,
		`
		INSERT INTO task (
			workflow_execution_id,
			step_index,
			step_name,
			input,
			state,
			scheduled_at,
			attempt,
			max_attempts
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id
		`,
		task.WorkflowExecutionID,
		task.StepIndex,
		task.StepName,
		task.Input,
		task.State,
		task.ScheduledAt,
		task.Attempt,
		task.MaxAttempts,
	).Scan(&task.ID)
}