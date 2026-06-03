package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

type WorkflowService interface {
	RegisterWorkflow(name string) (*workflow.Workflow, error)
	StartWorkflow(workflowID string, taskQueue string, input []byte) (*workflow.Task, error)
	PollTask(ctx context.Context, taskQueue string) (*workflow.Task, error)
	CompleteTask(ctx context.Context, taskID string, result []byte, errString string) error
}

type workflowInteractor struct {
	workflowRepo workflow.WorkflowRepository
	taskRepo     workflow.TaskRepository
}

func NewWorkflowService(wRepo workflow.WorkflowRepository, tRepo workflow.TaskRepository) WorkflowService {
	return &workflowInteractor{
		workflowRepo: wRepo,
		taskRepo:     tRepo,
	}
}

func (i *workflowInteractor) RegisterWorkflow(name string) (*workflow.Workflow, error) {
	w := &workflow.Workflow{
		ID:        fmt.Sprintf("wf-%d", time.Now().UnixNano()),
		Name:      name,
		State:     workflow.StateCreated,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if err := i.workflowRepo.Save(w); err != nil {
		return nil, fmt.Errorf("failed to register workflow: %w", err)
	}

	return w, nil
}

func (i *workflowInteractor) StartWorkflow(workflowID string, taskQueue string, input []byte) (*workflow.Task, error) {
	w, err := i.workflowRepo.FindByID(workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to find workflow: %w", err)
	}
	if w == nil {
		return nil, fmt.Errorf("workflow not found: %s", workflowID)
	}

	if err := workflow.ValidateTransition(w.State, workflow.StateRunning); err != nil {
		return nil, fmt.Errorf("cannot start workflow: %w", err)
	}

	w.State = workflow.StateRunning
	w.UpdatedAt = time.Now()
	if err := i.workflowRepo.UpdateState(w.ID, w.State); err != nil {
		return nil, fmt.Errorf("failed to update workflow state: %w", err)
	}

	t := &workflow.Task{
		ID:          fmt.Sprintf("task-%d", time.Now().UnixNano()),
		WorkflowID:  w.ID,
		TaskQueue:   taskQueue,
		Name:        w.Name,
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