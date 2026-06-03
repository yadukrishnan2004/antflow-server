package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

type WorkflowService interface {
	RegisterWorkflow(name string) (*workflow.WorkflowDefinition, error)
	StartWorkflow(workflowName string, taskQueue string, input []byte) (*workflow.Task, error)
	PollTask(ctx context.Context, taskQueue string) (*workflow.Task, error)
	CompleteTask(ctx context.Context, taskID string, result []byte, errString string) error
	GetWorkflowResult(ctx context.Context, workflowID string) (*workflow.Task, error)
	CancelWorkflow(ctx context.Context, workflowID string) error
	GetHistory(ctx context.Context, workflowID string) ([]workflow.HistoryEvent, error)
}

type workflowInteractor struct {
	workflowRepo workflow.WorkflowRepository
	taskRepo     workflow.TaskRepository
	historyRepo  workflow.HistoryRepository
}

func NewWorkflowService(wRepo workflow.WorkflowRepository, tRepo workflow.TaskRepository, hRepo workflow.HistoryRepository) WorkflowService {
	return &workflowInteractor{
		workflowRepo: wRepo,
		taskRepo:     tRepo,
		historyRepo:  hRepo,
	}
}

func (i *workflowInteractor) RegisterWorkflow(name string) (*workflow.WorkflowDefinition, error) {
	def := &workflow.WorkflowDefinition{
		Name:      name,
		CreatedAt: time.Time{}, // Just placeholder, db sets it or we can set it
	}

	err := i.workflowRepo.SaveDefinition(def)
	if err != nil {
		return nil, fmt.Errorf("failed to save workflow definition: %w", err)
	}

	return def, nil
}

func (i *workflowInteractor) StartWorkflow(workflowName string, taskQueue string, input []byte) (*workflow.Task, error) {
	// 1. Ensure definition exists
	_, err := i.workflowRepo.FindDefinitionByName(workflowName)
	if err != nil {
		return nil, fmt.Errorf("workflow definition not found: %w", err)
	}

	// 2. Create Execution
	exec := &workflow.WorkflowExecution{
		ID:        fmt.Sprintf("run-%s", uuid.New().String()),
		Name:      workflowName,
		State:     workflow.StateRunning,
		Input:     input,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	err = i.workflowRepo.SaveExecution(exec)
	if err != nil {
		return nil, fmt.Errorf("failed to save workflow execution: %w", err)
	}

	t := &workflow.Task{
		ID:          fmt.Sprintf("task-%s", uuid.New().String()),
		WorkflowExecutionID: exec.ID,
		TaskQueue:           taskQueue,
		Name:                workflowName,
		Input:       input,
		State:       workflow.StateCreated, // Should be pending or created. We map pending to "pending" in postgres_repository query for FindAndLockPendingTask.
	}

	if err := i.taskRepo.SaveTask(t); err != nil {
		return nil, fmt.Errorf("failed to save task: %w", err)
	}

	return t, nil
}

func (i *workflowInteractor) PollTask(ctx context.Context, taskQueue string) (*workflow.Task, error) {
	return i.taskRepo.FindAndLockPendingTask(taskQueue)
}

func (i *workflowInteractor) CompleteTask(ctx context.Context, taskID string, result []byte, errString string) error {
	return i.taskRepo.UpdateTaskComplete(taskID, result, errString)
}

func (i *workflowInteractor) GetWorkflowResult(ctx context.Context, workflowID string) (*workflow.Task, error) {
	return i.taskRepo.FindLatestTask(workflowID)
}

func (i *workflowInteractor) CancelWorkflow(ctx context.Context, workflowID string) error {
	exec, err := i.workflowRepo.FindExecutionByID(workflowID)
	if err != nil {
		return fmt.Errorf("failed to find execution: %w", err)
	}

	if exec.State == workflow.StateCompleted || exec.State == workflow.StateFailed || exec.State == workflow.StateCancelled {
		return fmt.Errorf("cannot cancel workflow in terminal state: %s", exec.State)
	}

	return i.workflowRepo.UpdateExecutionState(workflowID, workflow.StateCancelled)
}

func (i *workflowInteractor) GetHistory(ctx context.Context, workflowID string) ([]workflow.HistoryEvent, error) {
	return i.historyRepo.GetHistory(workflowID)
}