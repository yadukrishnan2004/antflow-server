package workflow

import "time"

// WorkflowDefinitionStep represents a step inside a registered workflow type.
type WorkflowDefinitionStep struct {
	WorkflowName   string
	StepIndex      int
	StepName       string
	TaskQueue      string
	TimeoutSeconds int
}

// WorkflowDefinition represents a registered workflow type.
type WorkflowDefinition struct {
	Name        string
	Description string
	Steps       []WorkflowDefinitionStep
	CreatedAt   time.Time
}

// WorkflowExecution represents a single run of a workflow.
type WorkflowExecution struct {
	ID               string
	WorkflowName     string
	TaskQueue        string
	Input            []byte
	Result           []byte
	State            State
	Error            string
	CurrentStepIndex int
	TotalSteps       int
	CreatedAt        time.Time
	UpdatedAt        time.Time
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

// Checkpoint represents an intermediate state of a workflow execution.
type Checkpoint struct {
	ID                  int64
	WorkflowExecutionID string
	StepIndex           int
	StateSnapshot       []byte
	CreatedAt           time.Time
}

// HistoryEvent represents an event in the workflow execution
type HistoryEvent struct {
	EventID             int64
	WorkflowExecutionID string
	StepIndex           *int
	StepName            *string
	EventType           string
	Payload             []byte
	Error               string
	CreatedAt           time.Time
}
