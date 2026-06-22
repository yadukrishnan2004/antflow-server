package usecase

import (
	"context"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

type WorkflowService interface {
	RegisterWorkflow(ctx context.Context, name string, workflowType string, stepNames []string, compensationStepNames []string) (*workflow.WorkflowDefinition, error)
	StartWorkflow(ctx context.Context, workflowName string, taskQueue string, input []byte) (*workflow.WorkflowExecution, error)
	PollTask(ctx context.Context, taskQueue string) (*workflow.Task, error)
	CompleteTask(ctx context.Context, taskID string, result []byte, errString string) error
	PollCompensationTask(ctx context.Context, taskQueue string) (*workflow.CompensationTask, error)
	CompleteCompensationTask(ctx context.Context, taskID string, result []byte, errString string) error
	GetWorkflowResult(ctx context.Context, workflowID string) (*workflow.WorkflowExecution, error)
	CancelWorkflow(ctx context.Context, workflowID string) error
	GetHistory(ctx context.Context, workflowID string) ([]workflow.HistoryEvent, error)
	GetHistoryAfter(ctx context.Context, workflowID string, afterID int64) ([]workflow.HistoryEvent, error)
	GetWorkflowNameForExecution(ctx context.Context, executionID string) (string, error)
	SubscribeToQueue(taskQueue string) (<-chan struct{}, func())
}

type workflowInteractor struct {
	namespaceRepo        workflow.NamespaceRepository
	workflowDefRepo      workflow.WorkflowDefinitionRepository
	workflowStepRepo     workflow.WorkflowDefinitionStepRepository
	executionRepo        workflow.WorkflowExecutionRepository
	taskRepo             workflow.TaskRepository
	compensationTaskRepo workflow.CompensationTaskRepository
	historyRepo          workflow.HistoryEventRepository
	checkpointRepo       workflow.CheckpointRepository
	broker               *TaskBroker
	compBroker 			 *TaskBroker
}

func New(
	namespaceRepo workflow.NamespaceRepository,
	workflowDefRepo workflow.WorkflowDefinitionRepository,
	workflowStepRepo workflow.WorkflowDefinitionStepRepository,
	executionRepo workflow.WorkflowExecutionRepository,
	taskRepo workflow.TaskRepository,
	compensationTaskRepo workflow.CompensationTaskRepository,
	historyRepo workflow.HistoryEventRepository,
	checkpointRepo workflow.CheckpointRepository,
) WorkflowService {
	return &workflowInteractor{
		namespaceRepo:        namespaceRepo,
		workflowDefRepo:      workflowDefRepo,
		workflowStepRepo:     workflowStepRepo,
		executionRepo:        executionRepo,
		taskRepo:             taskRepo,
		compensationTaskRepo: compensationTaskRepo,
		historyRepo:          historyRepo,
		checkpointRepo:       checkpointRepo,
		broker:               NewTaskBroker(),
	}
}

func (i *workflowInteractor) SubscribeToQueue(taskQueue string) (<-chan struct{}, func()) {
	return i.broker.Subscribe(taskQueue)
}