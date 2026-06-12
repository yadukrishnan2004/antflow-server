package persistence

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (s *PostgresCheckpointRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS checkpoint (
			id                    SERIAL PRIMARY KEY,
			workflow_execution_id TEXT NOT NULL,
			step_index            INT NOT NULL,
			state_snapshot        BYTEA NOT NULL,
			created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			CONSTRAINT fk_checkpoint_execution
				FOREIGN KEY (workflow_execution_id)
				REFERENCES workflow_execution(id)
				ON DELETE CASCADE
		);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_checkpoint_exec_step ON checkpoint(workflow_execution_id, step_index);
	`)
	return err
}

func (s *PostgresCheckpointRepository) SaveCheckpoint(checkpoint *workflow.Checkpoint) error {
	err := s.db.QueryRow(`
		INSERT INTO checkpoint (workflow_execution_id, step_index, state_snapshot, created_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (workflow_execution_id, step_index) DO UPDATE SET state_snapshot = EXCLUDED.state_snapshot
		RETURNING id
	`, checkpoint.WorkflowExecutionID, checkpoint.StepIndex, checkpoint.StateSnapshot, checkpoint.CreatedAt).Scan(&checkpoint.ID)

	if err != nil {
		return fmt.Errorf("repo: failed to save checkpoint: %w", err)
	}

	return nil
}

func (s *PostgresCheckpointRepository) GetLatestCheckpoint(workflowExecutionID string) (*workflow.Checkpoint, error) {
	row := s.db.QueryRow(`
		SELECT id, workflow_execution_id, step_index, state_snapshot, created_at
		FROM checkpoint
		WHERE workflow_execution_id = $1
		ORDER BY step_index DESC
		LIMIT 1
	`, workflowExecutionID)

	var c workflow.Checkpoint
	if err := row.Scan(
		&c.ID,
		&c.WorkflowExecutionID,
		&c.StepIndex,
		&c.StateSnapshot,
		&c.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil // No checkpoint yet
		}
		return nil, fmt.Errorf("repo: failed to get latest checkpoint: %w", err)
	}
	return &c, nil
}
