package workflow

type WorkflowRepository interface {
	SaveDefinition(def *WorkflowDefinition) error
	FindDefinitionByName(name string) (*WorkflowDefinition, error)
	
	SaveExecution(exec *WorkflowExecution) error
	FindExecutionByID(id string) (*WorkflowExecution, error)
	UpdateExecutionState(id string, state State) error
	Migrate() error
}

type TaskRepository interface {
	SaveTask(task *Task) error
	FindPendingTasks(workflowExecutionID string) ([]Task, error)
	FindAndLockPendingTask(taskQueue string) (*Task, error)
	UpdateState(taskID string, state State) error
	UpdateTaskComplete(taskID string, result []byte, errString string) error
	FindLatestTask(workflowExecutionID string) (*Task, error)
	ResetTimedOutTasks() error
	Migrate() error
}

type HistoryRepository interface {
	SaveEvent(event *HistoryEvent) error
	GetHistory(workflowExecutionID string) ([]HistoryEvent, error)
	Migrate() error
}