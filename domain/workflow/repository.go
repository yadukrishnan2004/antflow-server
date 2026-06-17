package workflow

import "context"


type NamespaceRepository interface {
	Create(ctx context.Context, ns *Namespace) (error)
	GetByID(ctx context.Context, id string) (*Namespace, error)
	GetByName(ctx context.Context, name string) (*Namespace, error)
}

// WorkflowDefinitionRepository manages the static code blueprints.
type WorkflowDefinitionRepository interface {
    Create(ctx context.Context, def *WorkflowDefinition) error
    GetByID(ctx context.Context, id string) (*WorkflowDefinition, error)
    GetByNamespaceID(ctx context.Context, namespaceID string) ([]WorkflowDefinition, error)
}

// WorkflowDefinitionStepRepository tracks step definitions for sequential tasks.
type WorkflowDefinitionStepRepository interface {
	BatchCreate(ctx context.Context, steps *WorkflowDefinitionStep) error
	GetStepsByDefinitionID(ctx context.Context, definitionID string) ([]WorkflowDefinitionStep, error)
}

// WorkflowExecutionRepository tracks the state machine of live workflow runs.
type WorkflowExecutionRepository interface {
	Create(ctx context.Context, tx *sql.Tx, exec *WorkflowExecution) error
	GetByID(ctx context.Context, id string) (*WorkflowExecution, error)
	
	// AdvanceState handles strict optimistic locking concurrency checks using next_event_id
	AdvanceState(ctx context.Context, tx *sql.Tx, id string, expectedEventID int64, newState string, newNextEventID int64) error
	Complete(ctx context.Context, tx *sql.Tx, id string, result []byte, closeTime time.Time) error
	Fail(ctx context.Context, tx *sql.Tx, id string, errMessage string, closeTime time.Time) error
}

// TaskRepository handles processing constraints and execution states for individual workflow tasks.
type TaskRepository interface {
	Create(ctx context.Context, tx *sql.Tx, task *Task) error
	GetByID(ctx context.Context, id string) (*Task, error)
	UpdateState(ctx context.Context, tx *sql.Tx, id string, state string, output []byte, errMessage string) error
}

// HistoryEventRepository records the immutable event stream ledger.
type HistoryEventRepository interface {
	AppendEvents(ctx context.Context, tx *sql.Tx, executionID string, events []HistoryEvent) error
	GetHistoryStream(ctx context.Context, executionID string) ([]HistoryEvent, error)
}

// CheckpointRepository captures periodic memory state snapshots for long-running steps.
type CheckpointRepository interface {
	Save(ctx context.Context, tx *sql.Tx, checkpoint *Checkpoint) error
	GetLatestCheckpoint(ctx context.Context, executionID string, stepIndex int) (*Checkpoint, error)
}