package persistence

import (
	"context"
	"database/sql"
	"time"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

// Migrate creates the task table.
//
// Changes vs old schema:
//   - attempt DEFAULT changed from 1 → 0. A task starts with 0 attempts;
//     each time it is picked up by FindAndLockPending the attempt counter is
//     incremented, so after first pickup attempt = 1, which correctly reflects
//     "this is the first attempt". The old default of 1 caused the counter to
//     be 2 on the first real execution.
//   - CountCompleted and GetAllOutputs are GONE from this repo. Completed task
//     rows are deleted immediately, so counting them here would race. The
//     usecase now uses IncrementCompletedSteps on the execution row and reads
//     outputs from history_event instead.
//   - chk_task_state now allows 'CANCELLED'. CancelWorkflow marks pending and
//     in-flight task rows CANCELLED rather than deleting them, so a worker
//     that completes a task after its workflow was cancelled can be detected
//     and turned into a no-op instead of corrupting already-terminal state.
func (s *PostgresTaskRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE EXTENSION IF NOT EXISTS "pgcrypto";

		CREATE TABLE IF NOT EXISTS task (
			id                   TEXT        PRIMARY KEY,
			workflow_execution_id UUID       NOT NULL,

			step_index  INTEGER NOT NULL,
			step_name   TEXT    NOT NULL,
			task_queue  TEXT    NOT NULL DEFAULT 'default',

			input  BYTEA NOT NULL,
			output BYTEA,

			state TEXT NOT NULL DEFAULT 'CREATED',
			error TEXT,

			scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			started_at   TIMESTAMPTZ,
			completed_at TIMESTAMPTZ,
			locked_until TIMESTAMPTZ,

			attempt      INTEGER NOT NULL DEFAULT 0,
			max_attempts INTEGER NOT NULL DEFAULT 3,

			CONSTRAINT fk_task_workflow_execution
				FOREIGN KEY (workflow_execution_id)
				REFERENCES workflow_execution(id)
				ON DELETE CASCADE,

			CONSTRAINT uq_task_workflow_step
				UNIQUE (workflow_execution_id, step_index),

			CONSTRAINT chk_task_state
				CHECK (state IN ('CREATED','SCHEDULED','RUNNING','COMPLETED','FAILED','CANCELLED')),

			CONSTRAINT chk_task_attempt
				CHECK (attempt >= 0),

			CONSTRAINT chk_task_max_attempts
				CHECK (max_attempts > 0)
		);

		-- Widen the constraint for tables created before CANCELLED was added.
		-- IF EXISTS guards re-runs against fresh databases where the table was
		-- just created above with the constraint already in its final form.
		ALTER TABLE task DROP CONSTRAINT IF EXISTS chk_task_state;
		ALTER TABLE task ADD CONSTRAINT chk_task_state
			CHECK (state IN ('CREATED','SCHEDULED','RUNNING','COMPLETED','FAILED','CANCELLED'));

		CREATE INDEX IF NOT EXISTS idx_task_state_queue_scheduled
			ON task (state, task_queue, scheduled_at);
	`)
	return err
}

func (s *PostgresTaskRepository) Create(ctx context.Context, task *workflow.Task) error {
	_, err := getDB(ctx, s.db).ExecContext(ctx, `
		INSERT INTO task
			(id, workflow_execution_id, step_index, step_name, task_queue,
			 input, state, scheduled_at, attempt, max_attempts)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
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

func (s *PostgresTaskRepository) GetByID(ctx context.Context, id string) (*workflow.Task, error) {
	task := &workflow.Task{}
	var stateStr string
	var startedAt, completedAt, lockedUntil sql.NullTime

	err := s.db.QueryRowContext(ctx, `
		SELECT id, workflow_execution_id, step_index, step_name, task_queue,
		       input, output, state, COALESCE(error,''),
		       scheduled_at, started_at, completed_at, locked_until,
		       attempt, max_attempts
		FROM   task
		WHERE  id = $1
	`, id).Scan(
		&task.ID, &task.WorkflowExecutionID, &task.StepIndex, &task.StepName, &task.TaskQueue,
		&task.Input, &task.Output, &stateStr, &task.Error,
		&task.ScheduledAt, &startedAt, &completedAt, &lockedUntil,
		&task.Attempt, &task.MaxAttempts,
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

// FindAndLockPending selects and locks one eligible task row inside a single
// transaction, eliminating the SELECT→UPDATE race window that existed before.
//
// Eligibility:
//   - state = 'CREATED' and scheduled_at <= NOW()  (fresh task)
//   - state = 'RUNNING' and locked_until < NOW()   (heartbeat timed-out task)
//
// CANCELLED rows are never matched by either branch, so a cancelled
// workflow's remaining tasks simply stop being offered to workers.
//
// On success the row is updated to state='RUNNING', started_at=NOW(),
// locked_until=NOW()+5min, and attempt is incremented. The returned Task
// reflects the post-update state.
//
// Returns (nil, nil) when no eligible task exists.
func (s *PostgresTaskRepository) FindAndLockPending(
	ctx context.Context, taskQueue string,
) (*workflow.Task, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck — rollback on any non-commit path

	// FOR UPDATE SKIP LOCKED: skip rows already locked by another connection,
	// so concurrent pollers never block each other.
	row := tx.QueryRowContext(ctx, `
		SELECT id, workflow_execution_id, step_index, step_name,
		       task_queue, input, state, attempt, max_attempts, scheduled_at
		FROM   task
		WHERE  (
			(state = 'CREATED' AND task_queue = $1 AND scheduled_at <= NOW())
			OR
			(state = 'RUNNING' AND task_queue = $1 AND locked_until < NOW())
		)
		ORDER  BY scheduled_at ASC
		LIMIT  1
		FOR UPDATE SKIP LOCKED
	`, taskQueue)

	t := &workflow.Task{}
	var stateStr string
	err = row.Scan(
		&t.ID, &t.WorkflowExecutionID, &t.StepIndex, &t.StepName,
		&t.TaskQueue, &t.Input, &stateStr, &t.Attempt, &t.MaxAttempts,
		&t.ScheduledAt,
	)
	if err == sql.ErrNoRows {
		// No work available; rollback is a no-op here.
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	lockedUntil := time.Now().Add(5 * time.Minute)
	_, err = tx.ExecContext(ctx, `
		UPDATE task
		SET    state        = 'RUNNING',
		       started_at   = NOW(),
		       locked_until = $1,
		       attempt      = attempt + 1
		WHERE  id = $2
	`, lockedUntil, t.ID)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	t.State = workflow.StateRunning
	t.LockedUntil = lockedUntil
	t.StartedAt = time.Now()
	t.Attempt++ // mirror the DB increment

	return t, nil
}

func (s *PostgresTaskRepository) UpdateCompleted(
	ctx context.Context, id string, output []byte, errMsg string,
) error {
	state := workflow.StateCompleted
	if errMsg != "" {
		state = workflow.StateFailed
	}
	_, err := s.db.ExecContext(ctx,
		`UPDATE task SET state=$1, output=$2, error=$3, completed_at=NOW() WHERE id=$4`,
		string(state), output, errMsg, id,
	)
	return err
}

func (s *PostgresTaskRepository) Delete(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM task WHERE id = $1`, id)
	return err
}

func (s *PostgresTaskRepository) UpdateState(ctx context.Context, id string, state workflow.State) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE task SET state=$1, locked_until=NULL WHERE id=$2`,
		string(state), id)
	return err
}

// CancelByExecution marks every non-terminal task row (CREATED, SCHEDULED, or
// RUNNING) for the given execution as CANCELLED in a single statement. Rows
// already COMPLETED or FAILED are left untouched — they're terminal and
// represent real work that happened, not pending work to cancel.
func (s *PostgresTaskRepository) CancelByExecution(ctx context.Context, executionID string) error {
	_, err := getDB(ctx, s.db).ExecContext(ctx, `
		UPDATE task
		SET    state = 'CANCELLED', locked_until = NULL
		WHERE  workflow_execution_id = $1
		  AND  state IN ('CREATED','SCHEDULED','RUNNING')
	`, executionID)
	return err
}

// ResetTimedOutTasks is a background-maintenance helper — call it from a
// periodic goroutine (e.g. every minute). It returns stuck RUNNING rows to
// CREATED so the next FindAndLockPending can pick them up.
//
// Note: FindAndLockPending already handles timed-out rows directly, so this
// function is a safety net for edge cases (e.g. server crash before the next
// poll cycle).
func (s *PostgresTaskRepository) ResetTimedOutTasks() error {
	_, err := s.db.Exec(`
		UPDATE task
		SET    state = 'CREATED', locked_until = NULL
		WHERE  state = 'RUNNING' AND locked_until < NOW()
	`)
	return err
}

var _ workflow.TaskRepository = (*PostgresTaskRepository)(nil)