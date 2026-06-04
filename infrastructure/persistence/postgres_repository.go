package persistence

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
	_ "github.com/lib/pq"
)

// WorkflowExecution represents one workflow execution.
type WorkflowExecution struct {
	ID        string
	Name      string
	Input     []byte
	Result    []byte
	State     string
	Error     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// WorkflowDefinition represents a registered workflow type.
type WorkflowDefinition struct {
	Name      string
	CreatedAt time.Time
}

// Task is a single unit of work within a workflow.
type Task struct {
	ID                  string
	WorkflowExecutionID string
	TaskQueue           string
	Name                string
	StepName            string
	StepIndex           int
	Input               []byte
	Output              []byte
	State               string
	Error               string
	ScheduledAt         time.Time
	CompletedAt         time.Time
	LockedUntil         time.Time
}

// PostgresWorkflowRepository implements workflow.WorkflowRepository.
type PostgresWorkflowRepository struct {
	db *sql.DB
}

// PostgresTaskRepository implements workflow.TaskRepository.
type PostgresTaskRepository struct {
	db *sql.DB
}

// PostgresHistoryRepository implements workflow.HistoryRepository.
type PostgresHistoryRepository struct {
	db *sql.DB
}

// New opens a Postgres connection, runs migrations for all repositories,
// and returns them together.
func New(dsn string) (*PostgresWorkflowRepository, *PostgresTaskRepository, *PostgresHistoryRepository, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("store: failed to open database: %w", err)
	}

	wRepo := &PostgresWorkflowRepository{db: db}
	tRepo := &PostgresTaskRepository{db: db}

	hRepo := &PostgresHistoryRepository{db: db}

	if err := wRepo.Migrate(); err != nil {
		return nil, nil, nil, fmt.Errorf("store: workflow migration failed: %w", err)
	}

	if err := tRepo.Migrate(); err != nil {
		return nil, nil, nil, fmt.Errorf("store: task migration failed: %w", err)
	}

	if err := hRepo.Migrate(); err != nil {
		return nil, nil, nil, fmt.Errorf("store: history migration failed: %w", err)
	}

	return wRepo, tRepo, hRepo, nil
}

// ---------------------------------------------------------------------------
// PostgresWorkflowRepository
// ---------------------------------------------------------------------------

func (s *PostgresWorkflowRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS workflow_definition (
			name       TEXT PRIMARY KEY,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);

		CREATE TABLE IF NOT EXISTS workflow_definition_step (
			workflow_name TEXT NOT NULL,
			step_index    INT NOT NULL,
			step_name     TEXT NOT NULL,
			PRIMARY KEY (workflow_name, step_index),
			FOREIGN KEY (workflow_name) REFERENCES workflow_definition(name) ON DELETE CASCADE
		);

		CREATE TABLE IF NOT EXISTS workflow_execution (
			id         TEXT PRIMARY KEY,
			name       TEXT NOT NULL,
			input      BYTEA,
			result     BYTEA,
			state      TEXT NOT NULL,
			error      TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			CONSTRAINT fk_def
				FOREIGN KEY (name)
				REFERENCES workflow_definition(name)
				ON DELETE CASCADE
		);
	`)
	return err
}

func (s *PostgresWorkflowRepository) SaveDefinition(def *workflow.WorkflowDefinition) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT INTO workflow_definition (name, created_at)
		VALUES ($1, $2)
		ON CONFLICT (name) DO NOTHING
	`, def.Name, def.CreatedAt)

	if err != nil {
		return fmt.Errorf("repo: failed to save workflow definition: %w", err)
	}

	for i, step := range def.Steps {
		_, err = tx.Exec(`
			INSERT INTO workflow_definition_step (workflow_name, step_index, step_name)
			VALUES ($1, $2, $3)
			ON CONFLICT (workflow_name, step_index) DO NOTHING
		`, def.Name, i, step)
		if err != nil {
			return fmt.Errorf("repo: failed to save definition step: %w", err)
		}
	}

	return tx.Commit()
}

func (s *PostgresWorkflowRepository) FindDefinitionByName(name string) (*workflow.WorkflowDefinition, error) {
	row := s.db.QueryRow(`
		SELECT name, created_at
		FROM workflow_definition
		WHERE name = $1
	`, name)

	var d workflow.WorkflowDefinition
	if err := row.Scan(
		&d.Name,
		&d.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("repo: workflow definition not found")
		}
		return nil, fmt.Errorf("repo: failed to find definition: %w", err)
	}

	rows, err := s.db.Query(`
		SELECT step_name
		FROM workflow_definition_step
		WHERE workflow_name = $1
		ORDER BY step_index ASC
	`, name)
	if err != nil {
		return nil, fmt.Errorf("repo: failed to find steps: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var stepName string
		if err := rows.Scan(&stepName); err != nil {
			return nil, err
		}
		d.Steps = append(d.Steps, stepName)
	}

	return &d, nil
}

func (s *PostgresWorkflowRepository) SaveExecution(exec *workflow.WorkflowExecution) error {
	_, err := s.db.Exec(`
		INSERT INTO workflow_execution (id, name, input, result, state, error, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`,
		exec.ID,
		exec.Name,
		nullableBytes(exec.Input),
		nullableBytes(exec.Result),
		string(exec.State),
		nullableString(exec.Error),
		exec.CreatedAt,
		exec.UpdatedAt,
	)

	if err != nil {
		return fmt.Errorf("repo: failed to save workflow execution: %w", err)
	}

	return nil
}

func (s *PostgresWorkflowRepository) FindExecutionByID(id string) (*workflow.WorkflowExecution, error) {
	row := s.db.QueryRow(`
		SELECT id, name, input, result, state, error, created_at, updated_at
		FROM workflow_execution
		WHERE id = $1
	`, id)

	var w workflow.WorkflowExecution
	var state string
	var input, result []byte
	var execErr sql.NullString

	if err := row.Scan(
		&w.ID,
		&w.Name,
		&input,
		&result,
		&state,
		&execErr,
		&w.CreatedAt,
		&w.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("repo: workflow execution not found")
		}
		return nil, fmt.Errorf("repo: failed to find execution: %w", err)
	}

	w.Input = input
	w.Result = result
	w.State = workflow.State(state)
	if execErr.Valid {
		w.Error = execErr.String
	}

	return &w, nil
}

func (s *PostgresWorkflowRepository) UpdateExecutionState(id string, state workflow.State) error {
	_, err := s.db.Exec(`
		UPDATE workflow_execution
		SET state = $1, updated_at = NOW()
		WHERE id = $2
	`, string(state), id)

	if err != nil {
		return fmt.Errorf("repo: failed to update execution state: %w", err)
	}

	return nil
}

// ---------------------------------------------------------------------------
// PostgresTaskRepository
// ---------------------------------------------------------------------------

// Migrate creates the task table if it does not already exist.
// The task table has a foreign key back to the workflow table, so
// PostgresWorkflowRepository.Migrate() must be called first.
func (s *PostgresTaskRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS task (
			id            TEXT PRIMARY KEY,
			workflow_execution_id TEXT NOT NULL,
			task_queue    TEXT NOT NULL,
			name          TEXT NOT NULL,
			step_name     TEXT NOT NULL,
			step_index    INT NOT NULL,

			input         BYTEA NOT NULL,
			output        BYTEA,
			state         TEXT NOT NULL,
			error         TEXT,

			scheduled_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			completed_at  TIMESTAMPTZ,
			locked_until  TIMESTAMPTZ,

			CONSTRAINT fk_execution
				FOREIGN KEY (workflow_execution_id)
				REFERENCES workflow_execution(id)
				ON DELETE CASCADE
		);
	`)
	return err
}

// SaveTask inserts a new task row.
func (s *PostgresTaskRepository) SaveTask(task *workflow.Task) error {
	_, err := s.db.Exec(`
		INSERT INTO task (id, workflow_execution_id, task_queue, name, step_name, step_index, input, output, state, error, scheduled_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`,
		task.ID,
		task.WorkflowExecutionID,
		task.TaskQueue,
		task.Name,
		task.StepName,
		task.StepIndex,
		task.Input,
		nullableBytes(task.Output),
		string(task.State),
		nullableString(task.Error),
		task.ScheduledAt,
		nullableTime(task.CompletedAt),
	)

	if err != nil {
		return fmt.Errorf("repo: failed to save task: %w", err)
	}

	return nil
}

// FindPendingTasks returns all tasks for the given workflow that are in the
// "pending" state.
func (s *PostgresTaskRepository) FindPendingTasks(workflowExecutionID string) ([]workflow.Task, error) {
	rows, err := s.db.Query(`
		SELECT id, workflow_execution_id, task_queue, name, step_name, step_index, input, output, state, error, scheduled_at, completed_at
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
		var completedAt sql.NullTime

		if err := rows.Scan(
			&t.ID,
			&t.WorkflowExecutionID,
			&t.TaskQueue,
			&t.Name,
			&t.StepName,
			&t.StepIndex,
			&t.Input,
			&t.Output,
			&state,
			&taskErr,
			&t.ScheduledAt,
			&completedAt,
		); err != nil {
			return nil, fmt.Errorf("repo: failed to scan task row: %w", err)
		}

		t.State = workflow.State(state)
		if taskErr.Valid {
			t.Error = taskErr.String
		}
		if completedAt.Valid {
			t.CompletedAt = completedAt.Time
		}

		tasks = append(tasks, t)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repo: row iteration error: %w", err)
	}

	return tasks, nil
}

// UpdateState sets the state column for the given task ID.
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

// FindTaskByID returns a task by its ID.
func (s *PostgresTaskRepository) FindTaskByID(taskID string) (*workflow.Task, error) {
	row := s.db.QueryRow(`
		SELECT id, workflow_execution_id, task_queue, name, step_name, step_index, input, output, state, error, scheduled_at, completed_at
		FROM task
		WHERE id = $1
	`, taskID)

	var t workflow.Task
	var state string
	var taskErr sql.NullString
	var completedAt sql.NullTime

	if err := row.Scan(
		&t.ID,
		&t.WorkflowExecutionID,
		&t.TaskQueue,
		&t.Name,
		&t.StepName,
		&t.StepIndex,
		&t.Input,
		&t.Output,
		&state,
		&taskErr,
		&t.ScheduledAt,
		&completedAt,
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
	if completedAt.Valid {
		t.CompletedAt = completedAt.Time
	}

	return &t, nil
}

// FindAndLockPendingTask returns the oldest pending task for a given queue, updating its state to RUNNING.
func (s *PostgresTaskRepository) FindAndLockPendingTask(taskQueue string) (*workflow.Task, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("repo: failed to begin tx: %w", err)
	}
	defer tx.Rollback()

	row := tx.QueryRow(`
		SELECT id, workflow_execution_id, task_queue, name, step_name, step_index, input, output, state, error, scheduled_at, completed_at
		FROM task
		WHERE task_queue = $1 AND state = 'CREATED' AND (locked_until IS NULL OR locked_until < NOW())
		ORDER BY scheduled_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`, taskQueue)

	var t workflow.Task
	var state string
	var taskErr sql.NullString
	var completedAt sql.NullTime

	if err := row.Scan(
		&t.ID,
		&t.WorkflowExecutionID,
		&t.TaskQueue,
		&t.Name,
		&t.StepName,
		&t.StepIndex,
		&t.Input,
		&t.Output,
		&state,
		&taskErr,
		&t.ScheduledAt,
		&completedAt,
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
	if completedAt.Valid {
		t.CompletedAt = completedAt.Time
	}

	_, err = tx.Exec(`
		UPDATE task
		SET state = 'RUNNING', locked_until = NOW() + INTERVAL '5 minutes'
		WHERE id = $1
	`, t.ID)
	if err != nil {
		return nil, fmt.Errorf("repo: failed to update task to running: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("repo: failed to commit tx: %w", err)
	}

	return &t, nil
}

// UpdateTaskComplete marks a task as completed or failed and saves its output.
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

// FindLatestTask finds the most recently scheduled task for a workflow.
func (s *PostgresTaskRepository) FindLatestTask(workflowExecutionID string) (*workflow.Task, error) {
	row := s.db.QueryRow(`
		SELECT id, workflow_execution_id, task_queue, name, step_name, step_index, input, output, state, error, scheduled_at, completed_at
		FROM task
		WHERE workflow_execution_id = $1
		ORDER BY scheduled_at DESC
		LIMIT 1
	`, workflowExecutionID)

	var t workflow.Task
	var state string
	var taskErr sql.NullString
	var completedAt sql.NullTime

	if err := row.Scan(
		&t.ID,
		&t.WorkflowExecutionID,
		&t.TaskQueue,
		&t.Name,
		&t.StepName,
		&t.StepIndex,
		&t.Input,
		&t.Output,
		&state,
		&taskErr,
		&t.ScheduledAt,
		&completedAt,
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
	if completedAt.Valid {
		t.CompletedAt = completedAt.Time
	}

	return &t, nil
}

// ResetTimedOutTasks sets RUNNING tasks that have exceeded their locked_until back to CREATED
func (s *PostgresTaskRepository) ResetTimedOutTasks() error {
	_, err := s.db.Exec(`
		UPDATE task
		SET state = 'CREATED', locked_until = NULL
		WHERE state = 'RUNNING' AND locked_until < NOW()
	`)
	if err != nil {
		return fmt.Errorf("repo: failed to reset timed out tasks: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// PostgresHistoryRepository
// ---------------------------------------------------------------------------

func (s *PostgresHistoryRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS history_event (
			id            SERIAL PRIMARY KEY,
			workflow_execution_id TEXT NOT NULL,
			event_type    TEXT NOT NULL,
			activity_name TEXT NOT NULL,
			result        BYTEA,
			created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			CONSTRAINT fk_history_execution
				FOREIGN KEY (workflow_execution_id)
				REFERENCES workflow_execution(id)
				ON DELETE CASCADE
		);
	`)
	return err
}

func (s *PostgresHistoryRepository) SaveEvent(event *workflow.HistoryEvent) error {
	err := s.db.QueryRow(`
		INSERT INTO history_event (workflow_execution_id, event_type, activity_name, result, created_at)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id
	`, event.WorkflowExecutionID, event.EventType, event.ActivityName, nullableBytes(event.Result), event.CreatedAt).Scan(&event.EventID)

	if err != nil {
		return fmt.Errorf("repo: failed to save history event: %w", err)
	}

	return nil
}

func (s *PostgresHistoryRepository) GetHistory(workflowExecutionID string) ([]workflow.HistoryEvent, error) {
	rows, err := s.db.Query(`
		SELECT id, workflow_execution_id, event_type, activity_name, result, created_at
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
		var result []byte
		if err := rows.Scan(
			&e.EventID,
			&e.WorkflowExecutionID,
			&e.EventType,
			&e.ActivityName,
			&result,
			&e.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("repo: failed to scan history row: %w", err)
		}
		e.Result = result
		events = append(events, e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repo: row iteration error: %w", err)
	}

	return events, nil
}

// ---------------------------------------------------------------------------
// helpers for nullable columns
// ---------------------------------------------------------------------------

func nullableString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

func nullableBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}

func nullableTime(t time.Time) sql.NullTime {
	return sql.NullTime{Time: t, Valid: !t.IsZero()}
}

