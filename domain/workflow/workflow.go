package workflow

import "time"

// WorkflowDefinition represents a registered workflow type.
type WorkflowDefinition struct {
	Name      string
	CreatedAt time.Time
}

// WorkflowExecution represents a single run of a workflow.
type WorkflowExecution struct {
	ID        string
	Name      string
	Input     []byte
	Result    []byte
	State     State
	Error     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type TaskType string

const (
	TaskTypeWorkflow TaskType = "WORKFLOW"
	TaskTypeActivity TaskType = "ACTIVITY"
)

// Activity is a single unit of work within a workflow.
type Task struct {
	ID                  string
	WorkflowExecutionID string
	TaskQueue  string
	Name       string
	TaskType   TaskType
	Input      []byte
	Output     []byte
	State      State
	Error      string
	ScheduledAt time.Time
	CompletedAt time.Time
}

// HistoryEvent represents an event in the workflow execution
type HistoryEvent struct {
	EventID             int64
	WorkflowExecutionID string
	EventType           string
	ActivityName string
	Result       []byte
	CreatedAt    time.Time
}
