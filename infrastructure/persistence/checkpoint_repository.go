package persistence

import (
	"context"
	"database/sql"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (s *PostgresCheckpointRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS checkpoint (
			id                    BIGSERIAL   PRIMARY KEY,
			workflow_execution_id UUID        NOT NULL,
			step_index            INTEGER     NOT NULL,
			state_snapshot        BYTEA       NOT NULL,
			created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),

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

func (s *PostgresCheckpointRepository) Save(
	ctx context.Context, checkpoint *workflow.Checkpoint,
) error {
	return s.db.QueryRowContext(ctx, `
		INSERT INTO checkpoint (workflow_execution_id, step_index, state_snapshot)
		VALUES ($1, $2, $3)
		ON CONFLICT (workflow_execution_id, step_index)
		DO UPDATE SET state_snapshot = EXCLUDED.state_snapshot, created_at = NOW()
		RETURNING id, created_at
	`,
		checkpoint.WorkflowExecutionID,
		checkpoint.StepIndex,
		checkpoint.StateSnapshot,
	).Scan(&checkpoint.ID, &checkpoint.CreatedAt)
}

func (s *PostgresCheckpointRepository) GetLatest(
	ctx context.Context, executionID string, stepIndex int,
) (*workflow.Checkpoint, error) {
	cp := &workflow.Checkpoint{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id, workflow_execution_id, step_index, state_snapshot, created_at
		FROM   checkpoint
		WHERE  workflow_execution_id = $1 AND step_index = $2
		ORDER  BY id DESC
		LIMIT  1
	`, executionID, stepIndex).Scan(
		&cp.ID, &cp.WorkflowExecutionID, &cp.StepIndex,
		&cp.StateSnapshot, &cp.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, workflow.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return cp, nil
}

var _ workflow.CheckpointRepository = (*PostgresCheckpointRepository)(nil)