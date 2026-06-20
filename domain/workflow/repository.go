package workflow

import (
	"context"
)

type NamespaceRepository interface {
	Create(ctx context.Context, ns *Namespace) error
	GetByID(ctx context.Context, id string) (*Namespace, error)
	GetByName(ctx context.Context, name string) (*Namespace, error)
}

// WorkflowDefinitionRepository manages the static code blueprints.
type WorkflowDefinitionRepository interface {
	Create(ctx context.Context, def *WorkflowDefinition) error
	GetByID(ctx context.Context, id string) (*WorkflowDefinition, error)

	// GetByName returns the currently active definition for the given namespace
	// and name. Returns ErrNotFound if no active definition exists.
	GetByName(ctx context.Context, namespaceID string, name string) (*WorkflowDefinition, error)

	GetByNamespaceID(ctx context.Context, namespaceID string) ([]WorkflowDefinition, error)

	// Deactivate marks a definition as inactive so a new version can be
	// registered. Called during re-registration when the step list changes.
	Deactivate(ctx context.Context, id string) error
}

// WorkflowDefinitionStepRepository tracks step definitions for sequential tasks.
type WorkflowDefinitionStepRepository interface {
	// Create inserts a single step definition row.
	// Renamed from BatchCreate — the original name was misleading since it
	// inserted one row at a time in a loop, not a batch.
	Create(ctx context.Context, step *WorkflowDefinitionStep) error

	GetStepsByDefinitionID(ctx context.Context, definitionID string) ([]WorkflowDefinitionStep, error)
	GetByDefinitionAndIndex(ctx context.Context, definitionID string, stepIndex int) (*WorkflowDefinitionStep, error)
	GetCompensationSteps(ctx context.Context, definitionID string, upToStepIndex int) ([]WorkflowDefinitionStep, error)
}

// WorkflowExecutionRepository tracks the state machine of live workflow runs.
type WorkflowExecutionRepository interface {
	Create(ctx context.Context, exec *WorkflowExecution) error
	GetByID(ctx context.Context, id string) (*WorkflowExecution, error)
	UpdateState(ctx context.Context, id string, state State) error
	UpdateStepCursor(ctx context.Context, id string, nextStep int) error
	SaveResult(ctx context.Context, id string, result []byte) error

	// IncrementCompletedSteps atomically increments the completed_steps counter
	// for the given execution and returns the new value. This is the correct
	// way to check whether an IndependentWorkflow has finished — a single
	// atomic UPDATE avoids the race condition that exists when you COUNT(*)
	// task rows and then delete them from separate goroutines.
	IncrementCompletedSteps(ctx context.Context, id string) (newCount int, err error)
	IncrementCompensationDone(ctx context.Context, id string) (newCount int, err error)
	SetCompensationTotal(ctx context.Context, id string, total int) error
}

// TaskRepository handles processing constraints and execution states for individual workflow tasks.
type TaskRepository interface {
	Create(ctx context.Context, task *Task) error
	GetByID(ctx context.Context, id string) (*Task, error)

	// FindAndLockPending atomically selects and locks a pending task for the
	// given queue using SELECT ... FOR UPDATE SKIP LOCKED inside a transaction.
	// Returns nil, nil when no work is available — callers should wait for a
	// broker notification rather than busy-polling.
	FindAndLockPending(ctx context.Context, taskQueue string) (*Task, error)

	UpdateCompleted(ctx context.Context, id string, output []byte, errMsg string) error
	Delete(ctx context.Context, id string) error
	UpdateState(ctx context.Context, id string, state State) error

	// CancelByExecution marks every non-terminal task row (CREATED, SCHEDULED,
	// or RUNNING) belonging to the given execution as CANCELLED. Used by
	// CancelWorkflow so that:
	//   - a worker that is mid-flight on a task and later calls CompleteTask
	//     finds nothing to act on (the usecase layer checks execution state
	//     first, see workflowInteractor.CompleteTask), and
	//   - a worker that has not yet picked up the task will no longer see it
	//     returned from FindAndLockPending, since CANCELLED is excluded from
	//     that query's eligibility filter.
	// This does not delete rows — CANCELLED tasks remain for audit/debugging,
	// consistent with how workflow_execution rows are never deleted either.
	CancelByExecution(ctx context.Context, executionID string) error
}

type HistoryEventRepository interface {
	Append(ctx context.Context, event *HistoryEvent) error

	// GetByExecution returns all events for an execution, ordered by ID ascending.
	// Use for full history reads (e.g. GetWorkflowResult debugging).
	GetByExecution(ctx context.Context, executionID string) ([]HistoryEvent, error)

	// GetByExecutionAfter returns only events with ID > afterID, ordered by ID
	// ascending. Use this in StreamWorkflowHistory to avoid re-reading events
	// the client has already received — one DB query per second per stream
	// client reading all rows is O(events) every tick; this is O(new events).
	GetByExecutionAfter(ctx context.Context, executionID string, afterID int64) ([]HistoryEvent, error)

	// GetStepOutputs returns the payload from every STEP_COMPLETED event for
	// the given execution, ordered by step_index ascending. This replaces
	// TaskRepository.GetAllOutputs — the task rows are deleted after processing,
	// but history events are permanent, so this is the correct source of truth
	// for assembling IndependentWorkflow combined results.
	GetStepOutputs(ctx context.Context, executionID string) ([]TaskOutput, error)
}

type CheckpointRepository interface {
	Save(ctx context.Context, checkpoint *Checkpoint) error
	GetLatest(ctx context.Context, executionID string, stepIndex int) (*Checkpoint, error)
}

type CompensationTaskRepository interface {
	Create(ctx context.Context, task *CompensationTask) error
	GetByID(ctx context.Context, id string) (*CompensationTask, error)
	FindAndLockPending(ctx context.Context, taskQueue string) (*CompensationTask, error)
	UpdateCompleted(ctx context.Context, id string, output []byte, errMsg string) error
	Delete(ctx context.Context, id string) error
	GetPendingByExecution(ctx context.Context, executionID string) ([]CompensationTask, error)
}