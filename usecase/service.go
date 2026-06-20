package usecase

import (
	"context"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

type WorkflowService interface {
	RegisterWorkflow(ctx context.Context, name string, workflowType string, stepNames []string) (*workflow.WorkflowDefinition, error)
	StartWorkflow(ctx context.Context, workflowName string, taskQueue string, input []byte) (*workflow.WorkflowExecution, error)
	PollTask(ctx context.Context, taskQueue string) (*workflow.Task, error)
	CompleteTask(ctx context.Context, taskID string, result []byte, errString string) error
	GetWorkflowResult(ctx context.Context, workflowID string) (*workflow.WorkflowExecution, error)
	CancelWorkflow(ctx context.Context, workflowID string) error
	GetHistory(ctx context.Context, workflowID string) ([]workflow.HistoryEvent, error)

	// GetHistoryAfter returns only history events with ID > afterID for the
	// given execution. Exposes HistoryEventRepository.GetByExecutionAfter so
	// callers like StreamWorkflowHistory can poll incrementally instead of
	// re-reading the full history every tick.
	GetHistoryAfter(ctx context.Context, workflowID string, afterID int64) ([]workflow.HistoryEvent, error)

	GetWorkflowNameForExecution(ctx context.Context, executionID string) (string, error)
	SubscribeToQueue(taskQueue string) (<-chan struct{}, func())
}

type workflowInteractor struct {
	namespaceRepo    workflow.NamespaceRepository
	workflowDefRepo  workflow.WorkflowDefinitionRepository
	workflowStepRepo workflow.WorkflowDefinitionStepRepository
	executionRepo    workflow.WorkflowExecutionRepository
	taskRepo         workflow.TaskRepository
	historyRepo      workflow.HistoryEventRepository
	checkpointRepo   workflow.CheckpointRepository
	broker           *TaskBroker
}

func New(
	namespaceRepo workflow.NamespaceRepository,
	workflowDefRepo workflow.WorkflowDefinitionRepository,
	workflowStepRepo workflow.WorkflowDefinitionStepRepository,
	executionRepo workflow.WorkflowExecutionRepository,
	taskRepo workflow.TaskRepository,
	historyRepo workflow.HistoryEventRepository,
	checkpointRepo workflow.CheckpointRepository,
) WorkflowService {
	return &workflowInteractor{
		namespaceRepo:    namespaceRepo,
		workflowDefRepo:  workflowDefRepo,
		workflowStepRepo: workflowStepRepo,
		executionRepo:    executionRepo,
		taskRepo:         taskRepo,
		historyRepo:      historyRepo,
		checkpointRepo:   checkpointRepo,
		broker:           NewTaskBroker(),
	}
}

func (i *workflowInteractor) SubscribeToQueue(taskQueue string) (<-chan struct{}, func()) {
	return i.broker.Subscribe(taskQueue)
}