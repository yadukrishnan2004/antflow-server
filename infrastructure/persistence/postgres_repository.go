package persistence

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
	_ "github.com/lib/pq"
)

// Workflow represents one workflow execution.
type Workflow struct {
	ID        string
	Name      string
	State     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Task is a single unit of work within a workflow.
type Task struct {
	ID          string
	WorkflowID  string
	Name        string
	Input       []byte
	Output      []byte
	State       string
	Error       string
	ScheduledAt time.Time
	CompletedAt time.Time
}

// PostgresWorkflowRepository implements workflow.WorkflowRepository.
type PostgresWorkflowRepository struct {
	db *sql.DB
}

// PostgresTaskRepository implements workflow.TaskRepository.
type PostgresTaskRepository struct {
	db *sql.DB
}

// New opens a Postgres connection, runs migrations for both repositories,
// and returns them together.
func New(dsn string) (*PostgresWorkflowRepository, *PostgresTaskRepository, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("store: failed to open database: %w", err)
	}

	wRepo := &PostgresWorkflowRepository{db: db}
	tRepo := &PostgresTaskRepository{db: db}

	if err := wRepo.Migrate(); err != nil {
		return nil, nil, fmt.Errorf("store: workflow migration failed: %w", err)
	}

	if err := tRepo.Migrate(); err != nil {
		return nil, nil, fmt.Errorf("store: task migration failed: %w", err)
	}

	return wRepo, tRepo, nil
}

// ---------------------------------------------------------------------------
// PostgresWorkflowRepository
// ---------------------------------------------------------------------------

// Migrate creates the workflow table if it does not already exist.
func (s *PostgresWorkflowRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS workflow (
			id          TEXT PRIMARY KEY,
			name        TEXT NOT NULL,
			state       TEXT NOT NULL DEFAULT 'pending',
			created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
	`)
	return err
}

// Save inserts a new workflow row.
func (s *PostgresWorkflowRepository) Save(w *workflow.Workflow) error {
	_, err := s.db.Exec(`
		INSERT INTO workflow (id, name, state)
		VALUES ($1, $2, $3)
	`, w.ID, w.Name, w.State)

	if err != nil {
		return fmt.Errorf("repo: failed to save workflow: %w", err)
	}

	return nil
}

// FindByID returns the workflow with the given ID, or nil if not found.
func (s *PostgresWorkflowRepository) FindByID(id string) (*workflow.Workflow, error) {
	row := s.db.QueryRow(`
		SELECT id, name, state, created_at, updated_at
		FROM workflow
		WHERE id = $1
	`, id)

	w := &workflow.Workflow{}
	var state string
	err := row.Scan(
		&w.ID,
		&w.Name,
		&state,
		&w.CreatedAt,
		&w.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("repo: failed to fetch workflow: %w", err)
	}
	w.State = workflow.State(state)

	return w, nil
}

// UpdateState sets the state column for the given workflow ID.
func (s *PostgresWorkflowRepository) UpdateState(id string, state workflow.State) error {
	_, err := s.db.Exec(`
		UPDATE workflow
		SET state = $1, updated_at = NOW()
		WHERE id = $2
	`, string(state), id)

	if err != nil {
		return fmt.Errorf("repo: failed to update workflow state: %w", err)
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
			workflow_id   TEXT NOT NULL,
			task_queue    TEXT NOT NULL,
			name          TEXT NOT NULL,

			input         BYTEA NOT NULL,
			output        BYTEA,

			state         TEXT NOT NULL DEFAULT 'pending',
			error         TEXT,

			scheduled_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			completed_at  TIMESTAMPTZ,

			CONSTRAINT fk_workflow
				FOREIGN KEY (workflow_id)
				REFERENCES workflow(id)
				ON DELETE CASCADE
		);
	`)
	return err
}

// SaveTask inserts a new task row.
func (s *PostgresTaskRepository) SaveTask(task *workflow.Task) error {
	_, err := s.db.Exec(`
		INSERT INTO task (id, workflow_id, task_queue, name, input, output, state, error, scheduled_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`,
		task.ID,
		task.WorkflowID,
		task.TaskQueue,
		task.Name,
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
func (s *PostgresTaskRepository) FindPendingTasks(workflowID string) ([]workflow.Task, error) {
	rows, err := s.db.Query(`
		SELECT id, workflow_id, task_queue, name, input, output, state, error, scheduled_at, completed_at
		FROM task
		WHERE workflow_id = $1
		  AND state = 'CREATED'
		ORDER BY scheduled_at ASC
	`, workflowID)
	if err != nil {
		return nil, fmt.Errorf("repo: failed to query pending tasks: %w", err)
	}
	defer rows.Close()

	var tasks []workflow.Task
	for rows.Next() {
		var t workflow.Task
		var output []byte
		var input []byte
		var state string
		var taskErr sql.NullString
		var completedAt sql.NullTime

		if err := rows.Scan(
			&t.ID,
			&t.WorkflowID,
			&t.TaskQueue,
			&t.Name,
			&input,
			&output,
			&state,
			&taskErr,
			&t.ScheduledAt,
			&completedAt,
		); err != nil {
			return nil, fmt.Errorf("repo: failed to scan task row: %w", err)
		}

		t.Input = input
		t.Output = output
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

// FindAndLockPendingTask returns the oldest pending task for a given queue, updating its state to RUNNING.
func (s *PostgresTaskRepository) FindAndLockPendingTask(taskQueue string) (*workflow.Task, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("repo: failed to begin tx: %w", err)
	}
	defer tx.Rollback()

	row := tx.QueryRow(`
		SELECT id, workflow_id, task_queue, name, input, output, state, error, scheduled_at, completed_at
		FROM task
		WHERE task_queue = $1 AND state = 'CREATED'
		ORDER BY scheduled_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`, taskQueue)

	var t workflow.Task
	var output []byte
	var input []byte
	var state string
	var taskErr sql.NullString
	var completedAt sql.NullTime

	if err := row.Scan(
		&t.ID,
		&t.WorkflowID,
		&t.TaskQueue,
		&t.Name,
		&input,
		&output,
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

	t.Input = input
	t.Output = output
	t.State = workflow.StateRunning
	if taskErr.Valid {
		t.Error = taskErr.String
	}
	if completedAt.Valid {
		t.CompletedAt = completedAt.Time
	}

	_, err = tx.Exec(`
		UPDATE task
		SET state = 'RUNNING'
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

