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
	DefaultTimeoutSeconds  int
	CreatedAt    time.Time
}


type WorkflowDefinitionStep struct {
	ID                   string
	WorkflowDefinitionID string
	StepIndex            int
	StepName             string
	CompensationStepName string 
	TimeoutSeconds       int
	TaskQueue            string 
	MaxAttempts          int
}


type WorkflowExecution struct {
    ID                   string
    WorkflowDefinitionID string
    WorkflowName         string
    WorkflowType         WorkflowType
    TaskQueue            string
    TotalSteps           int
    CompletedSteps       int
    CurrentStep          int
    Input                []byte
    Result               []byte
    State                State
    Error                string
    CreatedAt            time.Time
    ScheduledAt          time.Time
    UpdatedAt            time.Time
    CompletedAt          *time.Time
    DeadlineAt           *time.Time 
    CompensationTotal    int
    CompensationDone     int
}

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
	TimeoutSeconds      int 
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