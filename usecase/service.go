package usecase

import (
	"context"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

type WorkflowService interface {
	RegisterNameSpace(ctx context.Context, name string) (string, error)
	RegisterWorkflow(ctx context.Context, name string, workflowType string, stepNames []string) (*workflow.WorkflowDefinition, error)
	StartWorkflow(ctx context.Context, workflowName string, taskQueue string, input []byte) (*workflow.WorkflowExecution, error)
	PollTask(ctx context.Context, taskQueue string) (*workflow.Task, error)
	CompleteTask(ctx context.Context, taskID string, result []byte, errString string) error
	GetWorkflowResult(ctx context.Context, workflowID string) (*workflow.WorkflowExecution, error)
	CancelWorkflow(ctx context.Context, workflowID string) error
	GetHistory(ctx context.Context, workflowID string) ([]workflow.HistoryEvent, error)
	GetWorkflowNameForExecution(ctx context.Context, executionID string) (string, error)
	SubscribeToQueue(taskQueue string) (chan struct{}, func())
}

type workflowInteractor struct {
	namespaceRepo    workflow.NamespaceRepository
	workflowRepo     workflow.WorkflowDefinitionRepository
	workflowStepRepo workflow.WorkflowDefinitionStepRepository
	executionRepo    workflow.WorkflowExecutionRepository
	taskRepo         workflow.TaskRepository
	historyRepo      workflow.HistoryEventRepository
	checkpointRepo   workflow.CheckpointRepository
	taskBroker       *TaskBroker
}

func New(
	namespaceRepo workflow.NamespaceRepository,
	workflowRepo workflow.WorkflowDefinitionRepository,
	workflowStepRepo workflow.WorkflowDefinitionStepRepository,
	executionRepo workflow.WorkflowExecutionRepository,
	taskRepo workflow.TaskRepository,
	historyRepo workflow.HistoryEventRepository,
	checkpointRepo workflow.CheckpointRepository,
	taskBroker *TaskBroker,
) WorkflowService {
	return &workflowInteractor{
		namespaceRepo:    namespaceRepo,
		workflowRepo:     workflowRepo,
		workflowStepRepo: workflowStepRepo,
		executionRepo:    executionRepo,
		taskRepo:         taskRepo,
		historyRepo:      historyRepo,
		checkpointRepo:   checkpointRepo,
		taskBroker:       taskBroker,
	}
}

func (i *workflowInteractor) SubscribeToQueue(taskQueue string) (chan struct{}, func()) {
	return i.taskBroker.Subscribe(taskQueue)
}
