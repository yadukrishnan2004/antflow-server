package workflow

import "time"

//representing one workflow execution.
type Workflow struct{
	ID   string
	Name string
	State State
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
	ID         string
	WorkflowID string
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
	EventID      int64
	WorkflowID   string
	EventType    string
	ActivityName string
	Result       []byte
	CreatedAt    time.Time
}
