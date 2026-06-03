package workflow

type WorkflowRepository interface {
	Save(workflow *Workflow) error
	FindByID(id string) (*Workflow, error)
	UpdateState(id string, state State) error
	Migrate() error
}

type TaskRepository interface {
	SaveTask(task *Task) error
	FindPendingTasks(workflowID string) ([]Task, error)
	FindAndLockPendingTask(taskQueue string) (*Task, error)
	UpdateState(taskID string, state State) error
	UpdateTaskComplete(taskID string, result []byte, errString string) error
	FindLatestTask(workflowID string) (*Task, error)
	ResetTimedOutTasks() error
	Migrate() error
}

type HistoryRepository interface {
	SaveEvent(event *HistoryEvent) error
	GetHistory(workflowID string) ([]HistoryEvent, error)
	Migrate() error
}