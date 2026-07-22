package persistence

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (s *PostgresHistoryEventRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS history_event (
			id                   BIGSERIAL   PRIMARY KEY,
			workflow_execution_id UUID       NOT NULL,

			step_index  INTEGER,
			step_name   TEXT,

			event_type  TEXT NOT NULL,

			payload BYTEA,
			error   TEXT,

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
	ctx context.Context, event *workflow.HistoryEvent,
) error {
	var errStr *string
	if event.Error != "" {
		errStr = &event.Error
	}
	return getDB(ctx, s.db).QueryRowContext(ctx, `
		INSERT INTO history_event
			(workflow_execution_id, step_index, step_name, event_type, payload, error)
		VALUES ($1,$2,$3,$4,$5,$6)
		RETURNING id, created_at
	`,
		event.WorkflowExecutionID,
		event.StepIndex,
		event.StepName,
		event.EventType,
		event.Payload,
		errStr,
	).Scan(&event.ID, &event.CreatedAt)
}

// GetByExecution returns all events for an execution in insertion order.
func (s *PostgresHistoryEventRepository) GetByExecution(
	ctx context.Context, executionID string,
) ([]workflow.HistoryEvent, error) {
	return s.queryEvents(ctx, `
		SELECT id, workflow_execution_id, step_index, step_name,
		       event_type, payload, COALESCE(error,''), created_at
		FROM   history_event
		WHERE  workflow_execution_id = $1
		ORDER  BY id ASC
	`, executionID)
}

// GetByExecutionAfter returns events whose id > afterID in insertion order.
// This allows StreamWorkflowHistory to resume without re-reading the full
// history on every poll tick — fixing the O(n²) reads of the old version.
func (s *PostgresHistoryEventRepository) GetByExecutionAfter(
	ctx context.Context, executionID string, afterID int64,
) ([]workflow.HistoryEvent, error) {
	return s.queryEvents(ctx, `
		SELECT id, workflow_execution_id, step_index, step_name,
		       event_type, payload, COALESCE(error,''), created_at
		FROM   history_event
		WHERE  workflow_execution_id = $1 AND id > $2
		ORDER  BY id ASC
	`, executionID, afterID)
}

// GetStepOutputs returns the payload of every STEP_COMPLETED event for an
// execution, ordered by step_index. This replaces GetAllOutputs on the task
// repo, which was broken because task rows are deleted before collection.
func (s *PostgresHistoryEventRepository) GetStepOutputs(
	ctx context.Context, executionID string,
) ([]workflow.TaskOutput, error) {
	rows, err := getDB(ctx, s.db).QueryContext(ctx, `
		SELECT step_index, step_name, payload
		FROM   history_event
		WHERE  workflow_execution_id = $1
		  AND  event_type = 'STEP_COMPLETED'
		ORDER  BY step_index ASC
	`, executionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var outputs []workflow.TaskOutput
	for rows.Next() {
		var out workflow.TaskOutput
		var stepIndex sql.NullInt64
		var stepName sql.NullString
		var payload []byte

		if err := rows.Scan(&stepIndex, &stepName, &payload); err != nil {
			return nil, err
		}
		if stepIndex.Valid {
			out.StepIndex = int(stepIndex.Int64)
		}
		if stepName.Valid {
			out.StepName = stepName.String
		}
		// payload is already valid JSON (the worker serialised it); store as
		// RawMessage so it is not base64-encoded when we later marshal the
		// combined output slice.
		out.Output = json.RawMessage(payload)
		outputs = append(outputs, out)
	}
	return outputs, rows.Err()
}

// queryEvents is a shared scan helper used by GetByExecution and
// GetByExecutionAfter to avoid duplicating the scan logic.
func (s *PostgresHistoryEventRepository) queryEvents(
	ctx context.Context, query string, args ...any,
) ([]workflow.HistoryEvent, error) {
	rows, err := getDB(ctx, s.db).QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []workflow.HistoryEvent
	for rows.Next() {
		var ev workflow.HistoryEvent
		var stepIndex sql.NullInt64
		var stepName sql.NullString
		if err := rows.Scan(
			&ev.ID, &ev.WorkflowExecutionID,
			&stepIndex, &stepName,
			&ev.EventType, &ev.Payload, &ev.Error,
			&ev.CreatedAt,
		); err != nil {
			return nil, err
		}
		if stepIndex.Valid {
			v := int(stepIndex.Int64)
			ev.StepIndex = &v
		}
		if stepName.Valid {
			ev.StepName = &stepName.String
		}
		events = append(events, ev)
	}
	return events, rows.Err()
}

var _ workflow.HistoryEventRepository = (*PostgresHistoryEventRepository)(nil)