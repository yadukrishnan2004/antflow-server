package persistence

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
	_ "github.com/lib/pq"
)

// PostgresWorkflowRepository implements workflow.WorkflowRepository.
type PostgresWorkflowRepository struct {
	db *sql.DB
}

// PostgresTaskRepository implements workflow.TaskRepository.
type PostgresTaskRepository struct {
	db *sql.DB
}

// PostgresCheckpointRepository implements workflow.CheckpointRepository.
type PostgresCheckpointRepository struct {
	db *sql.DB
}

// PostgresHistoryRepository implements workflow.HistoryRepository.
type PostgresHistoryRepository struct {
	db *sql.DB
}

// New opens a Postgres connection, runs migrations for all repositories,
// and returns them together.
func New(dsn string) (*PostgresWorkflowRepository, *PostgresTaskRepository, *PostgresCheckpointRepository, *PostgresHistoryRepository, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("store: failed to open database: %w", err)
	}

	wRepo := &PostgresWorkflowRepository{db: db}
	tRepo := &PostgresTaskRepository{db: db}
	cRepo := &PostgresCheckpointRepository{db: db}
	hRepo := &PostgresHistoryRepository{db: db}

	if err := wRepo.Migrate(); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("store: workflow migration failed: %w", err)
	}

	if err := tRepo.Migrate(); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("store: task migration failed: %w", err)
	}

	if err := cRepo.Migrate(); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("store: checkpoint migration failed: %w", err)
	}

	if err := hRepo.Migrate(); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("store: history migration failed: %w", err)
	}

	return wRepo, tRepo, cRepo, hRepo, nil
}

// ---------------------------------------------------------------------------
// PostgresWorkflowRepository
// ---------------------------------------------------------------------------

func (s *PostgresWorkflowRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS workflow_definition (
			name        TEXT PRIMARY KEY,
			description TEXT,
			created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);

		CREATE TABLE IF NOT EXISTS workflow_definition_step (
			workflow_name   TEXT NOT NULL,
			step_index      INT NOT NULL,
			step_name       TEXT NOT NULL,
			task_queue      TEXT NOT NULL DEFAULT 'default',
			timeout_seconds INT NOT NULL DEFAULT 300,
			PRIMARY KEY (workflow_name, step_index),
			UNIQUE (workflow_name, step_name),
			FOREIGN KEY (workflow_name) REFERENCES workflow_definition(name) ON DELETE CASCADE
		);

		CREATE TABLE IF NOT EXISTS workflow_execution (
			id                 TEXT PRIMARY KEY,
			workflow_name      TEXT NOT NULL,
			task_queue         TEXT NOT NULL DEFAULT 'default',
			input              BYTEA,
			result             BYTEA,
			state              TEXT NOT NULL DEFAULT 'CREATED',
			error              TEXT,
			current_step_index INT NOT NULL DEFAULT 0,
			total_steps        INT NOT NULL,
			created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			CONSTRAINT fk_def
				FOREIGN KEY (workflow_name)
				REFERENCES workflow_definition(name)
				ON DELETE RESTRICT
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
		INSERT INTO workflow_definition (name, description, created_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (name) DO NOTHING
	`, def.Name, def.Description, def.CreatedAt)

	if err != nil {
		return fmt.Errorf("repo: failed to save workflow definition: %w", err)
	}

	return tx.Commit()
}

func (s *PostgresWorkflowRepository) FindDefinitionByName(name string) (*workflow.WorkflowDefinition, error) {
	row := s.db.QueryRow(`
		SELECT name, description, created_at
		FROM workflow_definition
		WHERE name = $1
	`, name)

	var d workflow.WorkflowDefinition
	var desc sql.NullString
	if err := row.Scan(
		&d.Name,
		&desc,
		&d.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("repo: workflow definition not found")
		}
		return nil, fmt.Errorf("repo: failed to find definition: %w", err)
	}
	if desc.Valid {
		d.Description = desc.String
	}

	rows, err := s.db.Query(`
		SELECT workflow_name, step_index, step_name, task_queue, timeout_seconds
		FROM workflow_definition_step
		WHERE workflow_name = $1
		ORDER BY step_index ASC
	`, name)
	if err != nil {
		return nil, fmt.Errorf("repo: failed to find steps: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var step workflow.WorkflowDefinitionStep
		if err := rows.Scan(&step.WorkflowName, &step.StepIndex, &step.StepName, &step.TaskQueue, &step.TimeoutSeconds); err != nil {
			return nil, err
		}
		d.Steps = append(d.Steps, step)
	}

	return &d, nil
}

func (s *PostgresWorkflowRepository) SaveExecution(exec *workflow.WorkflowExecution) error {
	_, err := s.db.Exec(`
		INSERT INTO workflow_execution (id, workflow_name, task_queue, input, result, state, error, current_step_index, total_steps, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`,
		exec.ID,
		exec.WorkflowName,
		exec.TaskQueue,
		nullableBytes(exec.Input),
		nullableBytes(exec.Result),
		string(exec.State),
		nullableString(exec.Error),
		exec.CurrentStepIndex,
		exec.TotalSteps,
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
		SELECT id, workflow_name, task_queue, input, result, state, error, current_step_index, total_steps, created_at, updated_at
		FROM workflow_execution
		WHERE id = $1
	`, id)

	var w workflow.WorkflowExecution
	var state string
	var input, result []byte
	var execErr sql.NullString

	if err := row.Scan(
		&w.ID,
		&w.WorkflowName,
		&w.TaskQueue,
		&input,
		&result,
		&state,
		&execErr,
		&w.CurrentStepIndex,
		&w.TotalSteps,
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

func (s *PostgresWorkflowRepository) FindStep(workflowName string, stepIndex int) (*workflow.WorkflowDefinitionStep, error) {
	row := s.db.QueryRow(`
		SELECT workflow_name, step_index, step_name, task_queue, timeout_seconds
		FROM workflow_definition_step
		WHERE workflow_name = $1 AND step_index = $2
	`,workflowName, stepIndex)

	var step workflow.WorkflowDefinitionStep

	if err := row.Scan(
		&step.WorkflowName, &step.StepIndex, &step.StepName,
        &step.TaskQueue, &step.TimeoutSeconds,
	);err != nil {
		if errors.Is(err, sql.ErrNoRows) {
            return nil, fmt.Errorf("repo: step %d not found for workflow %s", stepIndex, workflowName)
        }
		return nil, fmt.Errorf("repo: failed to find step: %w", err)
	}
	return &step, nil
}

func (s *PostgresWorkflowRepository) UpdateStepCursor(id string, stepIndex int) error {
	_, err := s.db.Exec(`
        UPDATE workflow_execution
        SET current_step_index = $1, updated_at = NOW()
        WHERE id = $2
    `, stepIndex, id)
    if err != nil {
        return fmt.Errorf("repo: failed to update step cursor: %w", err)
    }
    return nil
}

func (s *PostgresWorkflowRepository) SaveResult(id string, result []byte) error {
	_ , err := s.db.Exec(`
		UPDATE workflow_execution
		SET result = $1 , updated_at = NOW()
		WHERE id = $2	
	`, nullableBytes(result),id)

    if err != nil {
        return fmt.Errorf("repo: failed to save execution result: %w", err)
    }
    return nil
}



// ---------------------------------------------------------------------------
// PostgresTaskRepository
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// PostgresCheckpointRepository
// ---------------------------------------------------------------------------

func (s *PostgresCheckpointRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS checkpoint (
			id                    SERIAL PRIMARY KEY,
			workflow_execution_id TEXT NOT NULL,
			step_index            INT NOT NULL,
			state_snapshot        BYTEA NOT NULL,
			created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			CONSTRAINT fk_checkpoint_execution
				FOREIGN KEY (workflow_execution_id)
				REFERENCES workflow_execution(id)
				ON DELETE CASCADE
		);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_checkpoint_exec_step ON checkpoint(workflow_execution_id, step_index);
	`)
	return err
}

func (s *PostgresCheckpointRepository) SaveCheckpoint(checkpoint *workflow.Checkpoint) error {
	err := s.db.QueryRow(`
		INSERT INTO checkpoint (workflow_execution_id, step_index, state_snapshot, created_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (workflow_execution_id, step_index) DO UPDATE SET state_snapshot = EXCLUDED.state_snapshot
		RETURNING id
	`, checkpoint.WorkflowExecutionID, checkpoint.StepIndex, checkpoint.StateSnapshot, checkpoint.CreatedAt).Scan(&checkpoint.ID)

	if err != nil {
		return fmt.Errorf("repo: failed to save checkpoint: %w", err)
	}

	return nil
}

func (s *PostgresCheckpointRepository) GetLatestCheckpoint(workflowExecutionID string) (*workflow.Checkpoint, error) {
	row := s.db.QueryRow(`
		SELECT id, workflow_execution_id, step_index, state_snapshot, created_at
		FROM checkpoint
		WHERE workflow_execution_id = $1
		ORDER BY step_index DESC
		LIMIT 1
	`, workflowExecutionID)

	var c workflow.Checkpoint
	if err := row.Scan(
		&c.ID,
		&c.WorkflowExecutionID,
		&c.StepIndex,
		&c.StateSnapshot,
		&c.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil // No checkpoint yet
		}
		return nil, fmt.Errorf("repo: failed to get latest checkpoint: %w", err)
	}
	return &c, nil
}


// ---------------------------------------------------------------------------
// PostgresHistoryRepository
// ---------------------------------------------------------------------------

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
