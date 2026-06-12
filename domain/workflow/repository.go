package workflow

type WorkflowRepository interface {
    SaveDefinition(def *WorkflowDefinition) error
    FindDefinitionByName(name string) (*WorkflowDefinition, error)
    FindStep(workflowName string, stepIndex int) (*WorkflowDefinitionStep, error)
    
    SaveExecution(exec *WorkflowExecution) error
    FindExecutionByID(id string) (*WorkflowExecution, error)
    UpdateExecutionState(id string, state State) error
    UpdateStepCursor(id string, stepIndex int) error   
    SaveResult(id string, result []byte) error          
    GetWorkflowNameByExecutionID(executionID string) (string, error)
    Migrate() error
}

type TaskRepository interface {
	SaveTask(task *Task) error
	FindPendingTasks(workflowExecutionID string) ([]Task, error)
	FindTaskByID(taskID string) (*Task, error)
	FindAndLockPendingTask(taskQueue string) (*Task, error)
	UpdateState(taskID string, state State) error
	UpdateTaskComplete(taskID string, result []byte, errString string) error
	FindLatestTask(workflowExecutionID string) (*Task, error)
	ResetTimedOutTasks() error
	CountCompletedTasks(workflowExecutionID string) (int, error)
	GetAllTaskOutputs(workflowExecutionID string) ([]TaskOutput, error)
	Migrate() error
}

type CheckpointRepository interface {
	SaveCheckpoint(checkpoint *Checkpoint) error
	GetLatestCheckpoint(workflowExecutionID string) (*Checkpoint, error)
	Migrate() error
}

type HistoryRepository interface {
	SaveEvent(event *HistoryEvent) error
	GetHistory(workflowExecutionID string) ([]HistoryEvent, error)
	Migrate() error
}