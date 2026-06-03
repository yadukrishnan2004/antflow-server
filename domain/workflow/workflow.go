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

// Activity is a single unit of work within a workflow.
type Task struct {
	ID         string
	WorkflowID string
	TaskQueue  string
	Name       string
	Input      []byte
	Output     []byte
	State      State
	Error      string
	ScheduledAt time.Time
	CompletedAt time.Time
}

