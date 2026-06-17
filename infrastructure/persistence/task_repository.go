package persistence

import (
	"context"
	"database/sql"
	"time"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (s *PostgresTaskRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE EXTENSION IF NOT EXISTS "pgcrypto";

		CREATE TABLE IF NOT EXISTS task (
			id TEXT PRIMARY KEY,

			workflow_execution_id UUID NOT NULL,

			step_index INTEGER NOT NULL,
			step_name TEXT NOT NULL,
			task_queue TEXT NOT NULL DEFAULT 'default',

			input BYTEA NOT NULL,
			output BYTEA,

			state TEXT NOT NULL DEFAULT 'CREATED',
			error TEXT,

			scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			started_at TIMESTAMPTZ,
			completed_at TIMESTAMPTZ,
			locked_until TIMESTAMPTZ,

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

		CREATE INDEX IF NOT EXISTS idx_task_state_queue_scheduled
			ON task (state, task_queue, scheduled_at);
	`)
	return err
}

func (s *PostgresTaskRepository) Create(
	ctx context.Context,
	task *workflow.Task,
) error {
	_, err := s.db.ExecContext(
		ctx,
		`
		INSERT INTO task (
			id,
			workflow_execution_id,
			step_index,
			step_name,
			task_queue,
			input,
			state,
			scheduled_at,
			attempt,
			max_attempts
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		`,
		task.ID,
		task.WorkflowExecutionID,
		task.StepIndex,
		task.StepName,
		task.TaskQueue,
		task.Input,
		string(task.State),
		task.ScheduledAt,
		task.Attempt,
		task.MaxAttempts,
	)
	return err
}

func (s *PostgresTaskRepository) GetByID(
	ctx context.Context,
	id string,
) (*workflow.Task, error) {
	task := &workflow.Task{}
	var stateStr string
	var startedAt sql.NullTime
	var completedAt sql.NullTime
	var lockedUntil sql.NullTime

	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, workflow_execution_id, step_index, step_name, task_queue, input, output, state, COALESCE(error, ''), scheduled_at, started_at, completed_at, locked_until, attempt, max_attempts
		 FROM task
		 WHERE id = $1`,
		id,
	).Scan(
		&task.ID,
		&task.WorkflowExecutionID,
		&task.StepIndex,
		&task.StepName,
		&task.TaskQueue,
		&task.Input,
		&task.Output,
		&stateStr,
		&task.Error,
		&task.ScheduledAt,
		&startedAt,
		&completedAt,
		&lockedUntil,
		&task.Attempt,
		&task.MaxAttempts,
	)
	if err == sql.ErrNoRows {
		return nil, workflow.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	task.State = workflow.State(stateStr)
	if startedAt.Valid {
		task.StartedAt = startedAt.Time
	}
	if completedAt.Valid {
		task.CompletedAt = completedAt.Time
	}
	if lockedUntil.Valid {
		task.LockedUntil = lockedUntil.Time
	}

	return task, nil
}

func (s *PostgresTaskRepository) FindAndLockPending(
	ctx context.Context,
	taskQueue string,
) (*workflow.Task, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, workflow_execution_id, step_index, step_name,
		       task_queue, input, state, attempt, max_attempts, scheduled_at
		FROM task
		WHERE (state = 'CREATED' AND task_queue = $1 AND scheduled_at <= NOW())
		   OR (state = 'RUNNING' AND task_queue = $1 AND started_at < NOW() - INTERVAL '5 minutes')
		ORDER BY scheduled_at ASC
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, taskQueue)

	t := &workflow.Task{}
	var stateStr string
	err := row.Scan(
		&t.ID, &t.WorkflowExecutionID, &t.StepIndex, &t.StepName,
		&t.TaskQueue, &t.Input, &stateStr, &t.Attempt, &t.MaxAttempts,
		&t.ScheduledAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil // no work available
	}
	if err != nil {
		return nil, err
	}
	t.State = workflow.State(stateStr)

	// Mark it RUNNING immediately
	lockedUntil := time.Now().Add(5 * time.Minute)
	_, err = s.db.ExecContext(ctx,
		`UPDATE task SET state = 'RUNNING', started_at = NOW(), locked_until = $1, attempt = attempt + 1 WHERE id = $2`,
		lockedUntil, t.ID)
	if err != nil {
		return nil, err
	}

	t.State = workflow.StateRunning
	t.LockedUntil = lockedUntil
	t.StartedAt = time.Now()
	t.Attempt++

	return t, nil
}

func (s *PostgresTaskRepository) UpdateCompleted(
	ctx context.Context,
	id string,
	output []byte,
	errMsg string,
) error {
	state := "COMPLETED"
	if errMsg != "" {
		state = "FAILED"
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE task SET state=$1, output=$2, error=$3, completed_at=NOW() WHERE id=$4
	`, state, output, errMsg, id)
	return err
}

func (s *PostgresTaskRepository) Delete(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM task WHERE id = $1`, id)
	return err
}

func (s *PostgresTaskRepository) UpdateState(ctx context.Context, id string, state workflow.State) error {
	_, err := s.db.ExecContext(ctx, `UPDATE task SET state = $1, locked_until = NULL WHERE id = $2`, string(state), id)
	return err
}

func (s *PostgresTaskRepository) CountCompleted(
	ctx context.Context,
	executionID string,
) (int, error) {
	var count int
	err := s.db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM task WHERE workflow_execution_id = $1 AND state = 'COMPLETED'`,
		executionID,
	).Scan(&count)
	return count, err
}

func (s *PostgresTaskRepository) GetAllOutputs(
	ctx context.Context,
	executionID string,
) ([]workflow.TaskOutput, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT step_index, step_name, output
		 FROM task
		 WHERE workflow_execution_id = $1 AND state = 'COMPLETED'
		 ORDER BY step_index ASC`,
		executionID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var outputs []workflow.TaskOutput
	for rows.Next() {
		var out workflow.TaskOutput
		if err := rows.Scan(&out.StepIndex, &out.StepName, &out.Output); err != nil {
			return nil, err
		}
		outputs = append(outputs, out)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return outputs, nil
}

func (s *PostgresTaskRepository) ResetTimedOutTasks() error {
	_, err := s.db.Exec(`
		UPDATE task
		SET state = 'CREATED', locked_until = NULL
		WHERE state = 'RUNNING' AND locked_until < NOW()
	`)
	return err
}
