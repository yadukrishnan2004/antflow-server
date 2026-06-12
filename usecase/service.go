package usecase

import (
	"context"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

type WorkflowService interface {
	RegisterWorkflow(name string, workflowType string, stepNames []string) (*workflow.WorkflowDefinition, error)
	StartWorkflow(workflowName string, taskQueue string, input []byte) (*workflow.WorkflowExecution, error)
	PollTask(ctx context.Context, taskQueue string) (*workflow.Task, error)
	CompleteTask(ctx context.Context, taskID string, result []byte, errString string) error
	GetWorkflowResult(ctx context.Context, workflowID string) (*workflow.WorkflowExecution, error)
	CancelWorkflow(ctx context.Context, workflowID string) error
	GetHistory(ctx context.Context, workflowID string) ([]workflow.HistoryEvent, error)
	GetWorkflowNameForExecution(executionID string) (string, error)
}

type workflowInteractor struct {
	workflowRepo   workflow.WorkflowRepository
	taskRepo       workflow.TaskRepository
	checkpointRepo workflow.CheckpointRepository  
	historyRepo    workflow.HistoryRepository
}

func NewWorkflowService(
	wRepo workflow.WorkflowRepository,
	tRepo workflow.TaskRepository,
	cRepo workflow.CheckpointRepository,  
	hRepo workflow.HistoryRepository,
) WorkflowService {
	return &workflowInteractor{
		workflowRepo:   wRepo,
		taskRepo:       tRepo,
		checkpointRepo: cRepo,
		historyRepo:    hRepo,
	}
}
