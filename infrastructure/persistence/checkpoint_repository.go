package persistence

import (
	"context"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (s *PostgresCheckpointRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS checkpoint (
			id BIGSERIAL PRIMARY KEY,

			workflow_execution_id UUID NOT NULL,
			step_index INTEGER NOT NULL,

			state_snapshot BYTEA NOT NULL,

			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

			CONSTRAINT fk_checkpoint_workflow_execution
				FOREIGN KEY (workflow_execution_id)
				REFERENCES workflow_execution(id)
				ON DELETE CASCADE,

			CONSTRAINT uq_checkpoint_execution_step
				UNIQUE (workflow_execution_id, step_index),

			CONSTRAINT chk_checkpoint_step_index
				CHECK (step_index >= 0)
		);
	`)
	return err
}

func (s *PostgresCheckpointRepository) Create(
	ctx context.Context,
	checkpoint *workflow.Checkpoint,
) error {
	return s.db.QueryRowContext(
		ctx,
		`
		INSERT INTO checkpoint (
			workflow_execution_id,
			step_index,
			state_snapshot
		)
		VALUES ($1, $2, $3)
		RETURNING
			id,
			created_at
		`,
		checkpoint.WorkflowExecutionID,
		checkpoint.StepIndex,
		checkpoint.StateSnapshot,
	).Scan(
		&checkpoint.ID,
		&checkpoint.CreatedAt,
	)
}