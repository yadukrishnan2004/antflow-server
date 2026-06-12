package persistence

import (
	"database/sql"
	"fmt"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (s *PostgresHistoryRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS history_event (
			id                    SERIAL PRIMARY KEY,
			workflow_execution_id TEXT NOT NULL,
			step_index            INT,
			step_name             TEXT,
			event_type            TEXT NOT NULL,
			payload               BYTEA,
			error                 TEXT,
			created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			CONSTRAINT fk_history_execution
				FOREIGN KEY (workflow_execution_id)
				REFERENCES workflow_execution(id)
				ON DELETE CASCADE
		);
		CREATE INDEX IF NOT EXISTS idx_history_exec_id ON history_event(workflow_execution_id, id);
	`)
	return err
}

func (s *PostgresHistoryRepository) SaveEvent(event *workflow.HistoryEvent) error {
	err := s.db.QueryRow(`
		INSERT INTO history_event (workflow_execution_id, step_index, step_name, event_type, payload, error, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id
	`, event.WorkflowExecutionID, event.StepIndex, event.StepName, event.EventType, nullableBytes(event.Payload), nullableString(event.Error), event.CreatedAt).Scan(&event.EventID)

	if err != nil {
		return fmt.Errorf("repo: failed to save history event: %w", err)
	}

	return nil
}

func (s *PostgresHistoryRepository) GetHistory(workflowExecutionID string) ([]workflow.HistoryEvent, error) {
	rows, err := s.db.Query(`
		SELECT id, workflow_execution_id, step_index, step_name, event_type, payload, error, created_at
		FROM history_event
		WHERE workflow_execution_id = $1
		ORDER BY id ASC
	`, workflowExecutionID)
	if err != nil {
		return nil, fmt.Errorf("repo: failed to query history: %w", err)
	}
	defer rows.Close()

	var events []workflow.HistoryEvent
	for rows.Next() {
		var e workflow.HistoryEvent
		var payload []byte
		var stepIndex sql.NullInt32
		var stepName sql.NullString
		var eventErr sql.NullString

		if err := rows.Scan(
			&e.EventID,
			&e.WorkflowExecutionID,
			&stepIndex,
			&stepName,
			&e.EventType,
			&payload,
			&eventErr,
			&e.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("repo: failed to scan history row: %w", err)
		}
		
		if stepIndex.Valid {
			v := int(stepIndex.Int32)
			e.StepIndex = &v
		}
		if stepName.Valid {
			v := stepName.String
			e.StepName = &v
		}
		if eventErr.Valid {
			e.Error = eventErr.String
		}

		e.Payload = payload
		events = append(events, e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repo: row iteration error: %w", err)
	}

	return events, nil
}
