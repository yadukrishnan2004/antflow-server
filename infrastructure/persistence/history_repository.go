package persistence

import (
	"context"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (s *PostgresHistoryEventRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS history_event (
			id BIGSERIAL PRIMARY KEY,

			workflow_execution_id UUID NOT NULL,

			step_index INTEGER,
			step_name TEXT,

			event_type TEXT NOT NULL,

			payload BYTEA,
			error TEXT,

			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

			CONSTRAINT fk_history_event_workflow_execution
				FOREIGN KEY (workflow_execution_id)
				REFERENCES workflow_execution(id)
				ON DELETE CASCADE
		);

		CREATE INDEX IF NOT EXISTS idx_history_event_execution_id
			ON history_event (workflow_execution_id, id);
	`)
	return err
}

func (s *PostgresHistoryEventRepository) Create(
	ctx context.Context,
	event *workflow.HistoryEvent,
) error {
	return s.db.QueryRowContext(
		ctx,
		`
		INSERT INTO history_event (
			workflow_execution_id,
			step_index,
			step_name,
			event_type,
			payload,
			error
		)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING
			id,
			created_at
		`,
		event.WorkflowExecutionID,
		event.StepIndex,
		event.StepName,
		event.EventType,
		event.Payload,
		event.Error,
	).Scan(
		&event.ID,
		&event.CreatedAt,
	)
}