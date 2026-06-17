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
	GetByName(ctx context.Context, namespaceID string, name string) (*WorkflowDefinition, error)
	GetByNamespaceID(ctx context.Context, namespaceID string) ([]WorkflowDefinition, error)
}

// WorkflowDefinitionStepRepository tracks step definitions for sequential tasks.
type WorkflowDefinitionStepRepository interface {
	BatchCreate(ctx context.Context, steps *WorkflowDefinitionStep) error
	GetStepsByDefinitionID(ctx context.Context, definitionID string) ([]WorkflowDefinitionStep, error)
}

// WorkflowExecutionRepository tracks the state machine of live workflow runs.
type WorkflowExecutionRepository interface {
	Create(ctx context.Context, exec *WorkflowExecution) error
	GetByID(ctx context.Context, id string) (*WorkflowExecution, error)
	UpdateState(ctx context.Context, id string, state State) error
	UpdateStepCursor(ctx context.Context, id string, nextStep int) error
	SaveResult(ctx context.Context, id string, result []byte) error
}

// TaskRepository handles processing constraints and execution states for individual workflow tasks.
type TaskRepository interface {
	Create(ctx context.Context, task *Task) error
	GetByID(ctx context.Context, id string) (*Task, error)
	FindAndLockPending(ctx context.Context, taskQueue string) (*Task, error)
	UpdateCompleted(ctx context.Context, id string, output []byte, errMsg string) error
	CountCompleted(ctx context.Context, executionID string) (int, error)
	GetAllOutputs(ctx context.Context, executionID string) ([]TaskOutput, error)
}

// HistoryEventRepository records the immutable event stream ledger.
type HistoryEventRepository interface {
	Append(ctx context.Context, event *HistoryEvent) error
	GetByExecution(ctx context.Context, executionID string) ([]HistoryEvent, error)
}

// CheckpointRepository captures periodic memory state snapshots for long-running steps.
type CheckpointRepository interface {
	Save(ctx context.Context, checkpoint *Checkpoint) error
	GetLatest(ctx context.Context, executionID string, stepIndex int) (*Checkpoint, error)
}
