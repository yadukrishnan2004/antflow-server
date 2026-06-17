package workflow

import "time"

type WorkflowType string

const (
	ChainWorkflow       WorkflowType = "CHAIN"
	IndependentWorkflow WorkflowType = "INDEPENDENT"
)

// WorkflowDefinitionStep represents a step inside a registered workflow type.
type WorkflowDefinitionStep struct {
	ID                   string
	WorkflowDefinitionID string
	StepIndex            int
	StepName             string
	TimeoutSeconds       int
	TaskQueue            string // optional override; empty = use execution's queue
}

// Task is a single unit of work within a workflow.
type Task struct {
	ID                  string
	WorkflowExecutionID string
	StepIndex           int
	StepName            string
	TaskQueue           string
	Input               []byte
	Output              []byte
	State               State
	Error               string
	ScheduledAt         time.Time
	StartedAt           time.Time
	CompletedAt         time.Time
	LockedUntil         time.Time
	Attempt             int
	MaxAttempts         int
}

// HistoryEvent represents an event in the workflow execution
type HistoryEvent struct {
	ID                  int64
	WorkflowExecutionID string
	StepIndex           *int
	StepName            *string
	EventType           string
	Payload             []byte
	Error               string // empty string = no error
	CreatedAt           time.Time
}

type TaskOutput struct {
	StepIndex int    `json:"step_index"`
	StepName  string `json:"step_name"`
	Output    []byte `json:"output"`
}

type Namespace struct {
	ID        string
	Name      string
	CreatedAt time.Time
}

type WorkflowDefinition struct {
	ID           string
	NamespaceID  string
	Name         string
	WorkflowType string
	Steps        int
	CreatedAt    time.Time
}

type WorkflowExecution struct {
	ID                   string
	WorkflowDefinitionID string
	Input                []byte
	Result               []byte
	State                State
	Error                string
	CurrentStep          int
	CreatedAt            time.Time
	ScheduledAt          time.Time
	UpdatedAt            time.Time
	CompletedAt          *time.Time
	WorkflowName         string // denormalized for quick lookup
	TaskQueue            string // the queue this execution runs on
	TotalSteps           int    // total step count from the definition
	WorkflowType         WorkflowType
}

type Checkpoint struct {
	ID                  int64
	WorkflowExecutionID string
	StepIndex           int
	StateSnapshot       []byte
	CreatedAt           time.Time
}
