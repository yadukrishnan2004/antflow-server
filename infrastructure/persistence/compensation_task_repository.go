package persistence

import (
	"context"
	"database/sql"
	"time"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

type PostgresCompensationTaskRepository struct {
	db *sql.DB
}

func (s *PostgresCompensationTaskRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS compensation_task (
			id                   TEXT        PRIMARY KEY,
			workflow_execution_id UUID       NOT NULL,
			step_index           INTEGER     NOT NULL,
			step_name            TEXT        NOT NULL,
			compensation_step_name TEXT      NOT NULL,
			task_queue           TEXT        NOT NULL DEFAULT 'default',
			input                BYTEA,
			output               BYTEA,
			state                TEXT        NOT NULL DEFAULT 'CREATED',
			error                TEXT,
			attempt              INTEGER     NOT NULL DEFAULT 0,
			max_attempts         INTEGER     NOT NULL DEFAULT 3,
			scheduled_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			started_at           TIMESTAMPTZ,
			completed_at         TIMESTAMPTZ,
			locked_until         TIMESTAMPTZ,

			CONSTRAINT fk_compensation_task_execution
				FOREIGN KEY (workflow_execution_id)
				REFERENCES workflow_execution(id)
				ON DELETE CASCADE,

			CONSTRAINT uq_compensation_task_workflow_step
				UNIQUE (workflow_execution_id, step_index),

			CONSTRAINT chk_compensation_task_state
				CHECK (state IN ('CREATED','SCHEDULED','RUNNING','COMPLETED','FAILED','CANCELLED'))
		);

		-- Widen state check constraint in case it was created with old set
		ALTER TABLE compensation_task DROP CONSTRAINT IF EXISTS chk_compensation_task_state;
		ALTER TABLE compensation_task ADD CONSTRAINT chk_compensation_task_state
			CHECK (state IN ('CREATED','SCHEDULED','RUNNING','COMPLETED','FAILED','CANCELLED'));

		CREATE INDEX IF NOT EXISTS idx_compensation_task_state_queue_scheduled
			ON compensation_task (state, task_queue, scheduled_at);
	`)
	return err
}

func (s *PostgresCompensationTaskRepository) Create(ctx context.Context, task *workflow.CompensationTask) error {
	_, err := getDB(ctx, s.db).ExecContext(ctx, `
		INSERT INTO compensation_task
			(id, workflow_execution_id, step_index, step_name, compensation_step_name, task_queue,
			 input, state, scheduled_at, attempt, max_attempts)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
	`,
		task.ID,
		task.WorkflowExecutionID,
		task.StepIndex,
		task.StepName,
		task.CompensationStepName,
		task.TaskQueue,
		task.Input,
		string(task.State),
		task.ScheduledAt,
		task.Attempt,
		task.MaxAttempts,
	)
	return err
}

func (s *PostgresCompensationTaskRepository) GetByID(ctx context.Context, id string) (*workflow.CompensationTask, error) {
	task := &workflow.CompensationTask{}
	var stateStr string
	var startedAt, completedAt, lockedUntil sql.NullTime

	err := getDB(ctx, s.db).QueryRowContext(ctx, `
		SELECT id, workflow_execution_id, step_index, step_name, compensation_step_name, task_queue,
		       input, output, state, COALESCE(error,''),
		       scheduled_at, started_at, completed_at, locked_until,
		       attempt, max_attempts
		FROM   compensation_task
		WHERE  id = $1
	`, id).Scan(
		&task.ID, &task.WorkflowExecutionID, &task.StepIndex, &task.StepName, &task.CompensationStepName, &task.TaskQueue,
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

func (s *PostgresCompensationTaskRepository) FindAndLockPending(
	ctx context.Context, taskQueue string,
) (*workflow.CompensationTask, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx, `
		SELECT id, workflow_execution_id, step_index, step_name, compensation_step_name,
		       task_queue, input, state, attempt, max_attempts, scheduled_at
		FROM   compensation_task
		WHERE  (
			(state = 'CREATED' AND task_queue = $1 AND scheduled_at <= NOW())
			OR
			(state = 'RUNNING' AND task_queue = $1 AND locked_until < NOW())
		)
		ORDER  BY scheduled_at ASC
		LIMIT  1
		FOR UPDATE SKIP LOCKED
	`, taskQueue)

	t := &workflow.CompensationTask{}
	var stateStr string
	err = row.Scan(
		&t.ID, &t.WorkflowExecutionID, &t.StepIndex, &t.StepName, &t.CompensationStepName,
		&t.TaskQueue, &t.Input, &stateStr, &t.Attempt, &t.MaxAttempts,
		&t.ScheduledAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	lockedUntil := time.Now().Add(5 * time.Minute)
	_, err = tx.ExecContext(ctx, `
		UPDATE compensation_task
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
	t.Attempt++

	return t, nil
}

func (s *PostgresCompensationTaskRepository) UpdateCompleted(
	ctx context.Context, id string, output []byte, errMsg string,
) error {
	state := workflow.StateCompleted
	if errMsg != "" {
		state = workflow.StateFailed
	}
	_, err := getDB(ctx, s.db).ExecContext(ctx,
		`UPDATE compensation_task SET state=$1, output=$2, error=$3, completed_at=NOW() WHERE id=$4`,
		string(state), output, errMsg, id,
	)
	return err
}

func (s *PostgresCompensationTaskRepository) Delete(ctx context.Context, id string) error {
	_, err := getDB(ctx, s.db).ExecContext(ctx, `DELETE FROM compensation_task WHERE id = $1`, id)
	return err
}

func (s *PostgresCompensationTaskRepository) GetPendingByExecution(
	ctx context.Context, executionID string,
) ([]workflow.CompensationTask, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, workflow_execution_id, step_index, step_name, compensation_step_name, task_queue,
		       input, output, state, COALESCE(error,''),
		       scheduled_at, started_at, completed_at, locked_until,
		       attempt, max_attempts
		FROM   compensation_task
		WHERE  workflow_execution_id = $1 AND state IN ('CREATED', 'RUNNING', 'SCHEDULED')
		ORDER  BY step_index DESC
	`, executionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []workflow.CompensationTask
	for rows.Next() {
		var t workflow.CompensationTask
		var stateStr string
		var startedAt, completedAt, lockedUntil sql.NullTime
		if err := rows.Scan(
			&t.ID, &t.WorkflowExecutionID, &t.StepIndex, &t.StepName, &t.CompensationStepName, &t.TaskQueue,
			&t.Input, &t.Output, &stateStr, &t.Error,
			&t.ScheduledAt, &startedAt, &completedAt, &lockedUntil,
			&t.Attempt, &t.MaxAttempts,
		); err != nil {
			return nil, err
		}
		t.State = workflow.State(stateStr)
		if startedAt.Valid {
			t.StartedAt = startedAt.Time
		}
		if completedAt.Valid {
			t.CompletedAt = completedAt.Time
		}
		if lockedUntil.Valid {
			t.LockedUntil = lockedUntil.Time
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

func (s *PostgresCompensationTaskRepository) CancelByExecution(ctx context.Context, executionID string) error {
	_, err := getDB(ctx, s.db).ExecContext(ctx, `
		UPDATE compensation_task
		SET    state = 'CANCELLED', locked_until = NULL
		WHERE  workflow_execution_id = $1
		  AND  state IN ('CREATED','SCHEDULED','RUNNING')
	`, executionID)
	return err
}

var _ workflow.CompensationTaskRepository = (*PostgresCompensationTaskRepository)(nil)
