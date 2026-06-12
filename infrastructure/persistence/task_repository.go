package persistence

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (s *PostgresTaskRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS task (
			id                    TEXT PRIMARY KEY,
			workflow_execution_id TEXT NOT NULL,
			step_index            INT NOT NULL,
			step_name             TEXT NOT NULL,
			task_queue            TEXT NOT NULL,
			input                 BYTEA NOT NULL,
			output                BYTEA,
			state                 TEXT NOT NULL DEFAULT 'CREATED',
			error                 TEXT,
			scheduled_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			started_at            TIMESTAMPTZ,
			completed_at          TIMESTAMPTZ,
			locked_until          TIMESTAMPTZ,
			attempt               INT NOT NULL DEFAULT 1,
			max_attempts          INT NOT NULL DEFAULT 3,

			CONSTRAINT fk_execution
				FOREIGN KEY (workflow_execution_id)
				REFERENCES workflow_execution(id)
				ON DELETE CASCADE
		);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_task_exec_step ON task(workflow_execution_id, step_index);
		CREATE INDEX IF NOT EXISTS idx_task_queue_state ON task(task_queue, state, scheduled_at);
	`)
	return err
}

func (s *PostgresTaskRepository) SaveTask(task *workflow.Task) error {
	_, err := s.db.Exec(`
		INSERT INTO task (id, workflow_execution_id, step_index, step_name, task_queue, input, output, state, error, scheduled_at, started_at, completed_at, locked_until, attempt, max_attempts)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
	`,
		task.ID,
		task.WorkflowExecutionID,
		task.StepIndex,
		task.StepName,
		task.TaskQueue,
		task.Input,
		nullableBytes(task.Output),
		string(task.State),
		nullableString(task.Error),
		task.ScheduledAt,
		nullableTime(task.StartedAt),
		nullableTime(task.CompletedAt),
		nullableTime(task.LockedUntil),
		task.Attempt,
		task.MaxAttempts,
	)

	if err != nil {
		return fmt.Errorf("repo: failed to save task: %w", err)
	}

	return nil
}

func (s *PostgresTaskRepository) FindPendingTasks(workflowExecutionID string) ([]workflow.Task, error) {
	rows, err := s.db.Query(`
		SELECT id, workflow_execution_id, step_index, step_name, task_queue, input, output, state, error, scheduled_at, started_at, completed_at, locked_until, attempt, max_attempts
		FROM task
		WHERE workflow_execution_id = $1
		  AND state = 'CREATED'
		ORDER BY scheduled_at ASC
	`, workflowExecutionID)
	if err != nil {
		return nil, fmt.Errorf("repo: failed to query pending tasks: %w", err)
	}
	defer rows.Close()

	var tasks []workflow.Task
	for rows.Next() {
		var t workflow.Task
		var state string
		var taskErr sql.NullString
		var startedAt, completedAt, lockedUntil sql.NullTime

		if err := rows.Scan(
			&t.ID,
			&t.WorkflowExecutionID,
			&t.StepIndex,
			&t.StepName,
			&t.TaskQueue,
			&t.Input,
			&t.Output,
			&state,
			&taskErr,
			&t.ScheduledAt,
			&startedAt,
			&completedAt,
			&lockedUntil,
			&t.Attempt,
			&t.MaxAttempts,
		); err != nil {
			return nil, fmt.Errorf("repo: failed to scan task row: %w", err)
		}

		t.State = workflow.State(state)
		if taskErr.Valid {
			t.Error = taskErr.String
		}
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

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repo: row iteration error: %w", err)
	}

	return tasks, nil
}

func (s *PostgresTaskRepository) UpdateState(taskID string, state workflow.State) error {
	_, err := s.db.Exec(`
		UPDATE task
		SET state = $1
		WHERE id = $2
	`, string(state), taskID)

	if err != nil {
		return fmt.Errorf("repo: failed to update task state: %w", err)
	}

	return nil
}

func (s *PostgresTaskRepository) FindTaskByID(taskID string) (*workflow.Task, error) {
	row := s.db.QueryRow(`
		SELECT id, workflow_execution_id, step_index, step_name, task_queue, input, output, state, error, scheduled_at, started_at, completed_at, locked_until, attempt, max_attempts
		FROM task
		WHERE id = $1
	`, taskID)

	var t workflow.Task
	var state string
	var taskErr sql.NullString
	var startedAt, completedAt, lockedUntil sql.NullTime

	if err := row.Scan(
		&t.ID,
		&t.WorkflowExecutionID,
		&t.StepIndex,
		&t.StepName,
		&t.TaskQueue,
		&t.Input,
		&t.Output,
		&state,
		&taskErr,
		&t.ScheduledAt,
		&startedAt,
		&completedAt,
		&lockedUntil,
		&t.Attempt,
		&t.MaxAttempts,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("repo: task not found")
		}
		return nil, fmt.Errorf("repo: failed to find task by id: %w", err)
	}

	t.State = workflow.State(state)
	if taskErr.Valid {
		t.Error = taskErr.String
	}
	if startedAt.Valid {
		t.StartedAt = startedAt.Time
	}
	if completedAt.Valid {
		t.CompletedAt = completedAt.Time
	}
	if lockedUntil.Valid {
		t.LockedUntil = lockedUntil.Time
	}

	return &t, nil
}

func (s *PostgresTaskRepository) FindAndLockPendingTask(taskQueue string) (*workflow.Task, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("repo: failed to begin tx: %w", err)
	}
	defer tx.Rollback()

	row := tx.QueryRow(`
		SELECT id, workflow_execution_id, step_index, step_name, task_queue, input, output, state, error, scheduled_at, started_at, completed_at, locked_until, attempt, max_attempts
		FROM task
		WHERE task_queue = $1 AND state = 'CREATED' AND (locked_until IS NULL OR locked_until < NOW())
		ORDER BY scheduled_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`, taskQueue)

	var t workflow.Task
	var state string
	var taskErr sql.NullString
	var startedAt, completedAt, lockedUntil sql.NullTime

	if err := row.Scan(
		&t.ID,
		&t.WorkflowExecutionID,
		&t.StepIndex,
		&t.StepName,
		&t.TaskQueue,
		&t.Input,
		&t.Output,
		&state,
		&taskErr,
		&t.ScheduledAt,
		&startedAt,
		&completedAt,
		&lockedUntil,
		&t.Attempt,
		&t.MaxAttempts,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil // No tasks available
		}
		return nil, fmt.Errorf("repo: failed to lock task: %w", err)
	}

	t.State = workflow.StateRunning
	if taskErr.Valid {
		t.Error = taskErr.String
	}
	if startedAt.Valid {
		t.StartedAt = startedAt.Time
	} else {
		t.StartedAt = time.Now()
	}
	if completedAt.Valid {
		t.CompletedAt = completedAt.Time
	}
	if lockedUntil.Valid {
		t.LockedUntil = lockedUntil.Time
	}

	_, err = tx.Exec(`
		UPDATE task
		SET state = 'RUNNING', started_at = $1, locked_until = NOW() + INTERVAL '5 minutes'
		WHERE id = $2
	`, t.StartedAt, t.ID)
	if err != nil {
		return nil, fmt.Errorf("repo: failed to update task to running: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("repo: failed to commit tx: %w", err)
	}

	return &t, nil
}

func (s *PostgresTaskRepository) UpdateTaskComplete(taskID string, result []byte, errString string) error {
	state := workflow.StateCompleted
	if errString != "" {
		state = workflow.StateFailed
	}

	_, err := s.db.Exec(`
		UPDATE task
		SET state = $1, output = $2, error = $3, completed_at = NOW()
		WHERE id = $4
	`, string(state), nullableBytes(result), nullableString(errString), taskID)

	if err != nil {
		return fmt.Errorf("repo: failed to complete task: %w", err)
	}

	return nil
}

func (s *PostgresTaskRepository) FindLatestTask(workflowExecutionID string) (*workflow.Task, error) {
	row := s.db.QueryRow(`
		SELECT id, workflow_execution_id, step_index, step_name, task_queue, input, output, state, error, scheduled_at, started_at, completed_at, locked_until, attempt, max_attempts
		FROM task
		WHERE workflow_execution_id = $1
		ORDER BY scheduled_at DESC
		LIMIT 1
	`, workflowExecutionID)

	var t workflow.Task
	var state string
	var taskErr sql.NullString
	var startedAt, completedAt, lockedUntil sql.NullTime

	if err := row.Scan(
		&t.ID,
		&t.WorkflowExecutionID,
		&t.StepIndex,
		&t.StepName,
		&t.TaskQueue,
		&t.Input,
		&t.Output,
		&state,
		&taskErr,
		&t.ScheduledAt,
		&startedAt,
		&completedAt,
		&lockedUntil,
		&t.Attempt,
		&t.MaxAttempts,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("repo: failed to find latest task: %w", err)
	}

	t.State = workflow.State(state)
	if taskErr.Valid {
		t.Error = taskErr.String
	}
	if startedAt.Valid {
		t.StartedAt = startedAt.Time
	}
	if completedAt.Valid {
		t.CompletedAt = completedAt.Time
	}
	if lockedUntil.Valid {
		t.LockedUntil = lockedUntil.Time
	}

	return &t, nil
}

func (s *PostgresTaskRepository) ResetTimedOutTasks() error {
	_, err := s.db.Exec(`
		UPDATE task
		SET state = 'CREATED', locked_until = NULL, attempt = attempt + 1
		WHERE state = 'RUNNING' AND locked_until < NOW() AND attempt < max_attempts
	`)
	if err != nil {
		return fmt.Errorf("repo: failed to reset timed out tasks: %w", err)
	}

	_, err = s.db.Exec(`
		UPDATE task
		SET state = 'FAILED', error = 'task timed out and reached max attempts', completed_at = NOW()
		WHERE state = 'RUNNING' AND locked_until < NOW() AND attempt >= max_attempts
	`)
	if err != nil {
		return fmt.Errorf("repo: failed to fail timed out tasks: %w", err)
	}

	return nil
}

func (s *PostgresTaskRepository) CountCompletedTasks(workflowExecutionID string) (int, error) {
	row := s.db.QueryRow(`
		SELECT COUNT(*) FROM task
		WHERE workflow_execution_id = $1 AND state = 'COMPLETED'
	`, workflowExecutionID)
	var count int
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("repo: failed to count completed tasks: %w", err)
	}
	return count, nil
}

func (s *PostgresTaskRepository) GetAllTaskOutputs(workflowExecutionID string) ([]workflow.TaskOutput, error) {
	rows, err := s.db.Query(`
		SELECT step_index, step_name, output FROM task
		WHERE workflow_execution_id = $1
		ORDER BY step_index ASC
	`, workflowExecutionID)
	if err != nil {
		return nil, fmt.Errorf("repo: failed to query task outputs: %w", err)
	}
	defer rows.Close()

	var outputs []workflow.TaskOutput
	for rows.Next() {
		var o workflow.TaskOutput
		var outputBytes []byte
		if err := rows.Scan(&o.StepIndex, &o.StepName, &outputBytes); err != nil {
			return nil, err
		}
		o.Output = outputBytes
		outputs = append(outputs, o)
	}
	return outputs, nil
}
