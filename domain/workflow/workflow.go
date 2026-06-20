package workflow
 
import (
	"encoding/json"
	"time"
)


type WorkflowType string
 
const (
	ChainWorkflow       WorkflowType = "CHAIN"
	IndependentWorkflow WorkflowType = "INDEPENDENT"
	SagaWorkflow        WorkflowType = "SAGA"
)


type WorkflowDefinition struct {
	ID           string
	NamespaceID  string
	Name         string
	WorkflowType WorkflowType 
	Version      int
	IsActive     bool
	Steps        int 
	CreatedAt    time.Time
}


type WorkflowDefinitionStep struct {
	ID                   string
	WorkflowDefinitionID string
	StepIndex            int
	StepName             string
	CompensationStepName string // empty = no compensation for this step
	TimeoutSeconds       int
	TaskQueue            string // optional per-step override; empty = use execution queue
}


type WorkflowExecution struct {
	ID                   string
	WorkflowDefinitionID string
	WorkflowName         string       // denormalized for fast lookup without joining definition
	WorkflowType         WorkflowType // denormalized — avoids definition join on every task completion
	TaskQueue            string       // the queue this execution's tasks are dispatched to
	TotalSteps           int          // copied from definition at start time; definition may change later
	CompletedSteps       int          // atomically incremented; replaces CountCompleted queries on task table
	CurrentStep          int          // cursor for CHAIN workflows; unused for INDEPENDENT
	Input                []byte
	Result               []byte
	State                State
	Error                string
	CreatedAt            time.Time
	ScheduledAt          time.Time
	UpdatedAt            time.Time
	CompletedAt          *time.Time
	CompensationTotal    int          // total compensation tasks to run
	CompensationDone     int          // compensation tasks completed
}

type Task struct {
	ID                  string
	WorkflowExecutionID string
	StepIndex           int
	StepName            string
	TaskQueue           string
	Input               []byte
	Output              []byte // transient; only set while CompleteTask processes the result
	State               State
	Error               string
	ScheduledAt         time.Time
	StartedAt           time.Time
	CompletedAt         time.Time
	LockedUntil         time.Time
	Attempt             int // number of times this task has been attempted; starts at 0
	MaxAttempts         int
}


type HistoryEvent struct {
	ID                  int64
	WorkflowExecutionID string
	StepIndex           *int    // nil for workflow-level events
	StepName            *string // nil for workflow-level events
	EventType           EventType
	Payload             []byte
	Error               string // empty = no error
	CreatedAt           time.Time
}


type TaskOutput struct {
	StepIndex int             `json:"step_index"`
	StepName  string          `json:"step_name"`
	Output    json.RawMessage `json:"output"`
}

type Namespace struct {
	ID        string
	Name      string
	CreatedAt time.Time
}

type Checkpoint struct {
	ID                  int64
	WorkflowExecutionID string
	StepIndex           int
	StateSnapshot       []byte
	CreatedAt           time.Time
}

type CompensationTask struct {
	ID                   string
	WorkflowExecutionID  string
	StepIndex            int
	StepName             string
	CompensationStepName string
	TaskQueue            string
	Input                []byte
	Output               []byte
	State                State
	Error                string
	Attempt              int
	MaxAttempts          int
	ScheduledAt          time.Time
	StartedAt            time.Time
	CompletedAt          time.Time
	LockedUntil          time.Time
}