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

func (s *PostgresHistoryEventRepository) Append(
	ctx context.Context,
	event *workflow.HistoryEvent,
) error {
	var errStr *string
	if event.Error != "" {
		errStr = &event.Error
	}

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
		errStr,
	).Scan(
		&event.ID,
		&event.CreatedAt,
	)
}

func (s *PostgresHistoryEventRepository) GetByExecution(
	ctx context.Context,
	executionID string,
) ([]workflow.HistoryEvent, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT id, workflow_execution_id, step_index, step_name, event_type, payload, COALESCE(error, ''), created_at
		 FROM history_event
		 WHERE workflow_execution_id = $1
		 ORDER BY id ASC`,
		executionID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []workflow.HistoryEvent
	for rows.Next() {
		var event workflow.HistoryEvent
		if err := rows.Scan(
			&event.ID,
			&event.WorkflowExecutionID,
			&event.StepIndex,
			&event.StepName,
			&event.EventType,
			&event.Payload,
			&event.Error,
			&event.CreatedAt,
		); err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return events, nil
}
