package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (i *workflowInteractor) StartWorkflow(ctx context.Context, workflowName string, taskQueue string, input []byte) (*workflow.WorkflowExecution, error) {
	ns, err := i.namespaceRepo.GetByName(ctx, workflowName)
	if err != nil {
		return nil, fmt.Errorf("failed to get namespace: %w", err)
	}

	def, err := i.workflowRepo.GetByName(ctx, ns.ID, workflowName)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow definition: %w", err)
	}

	exec := &workflow.WorkflowExecution{
		ID:                   uuid.New().String(),
		WorkflowDefinitionID: def.ID,
		WorkflowName:         def.Name,
		TaskQueue:            taskQueue,
		TotalSteps:           def.Steps,
		WorkflowType:         workflow.WorkflowType(def.WorkflowType),
		Input:                input,
		State:                workflow.StateRunning, // Set state to RUNNING upon starting
		CurrentStep:          1,
		CreatedAt:            time.Now(),
		ScheduledAt:          time.Now(),
		UpdatedAt:            time.Now(),
	}

	if err := i.executionRepo.Create(ctx, exec); err != nil {
		return nil, fmt.Errorf("failed to create workflow execution: %w", err)
	}

	// Fetch all steps for this definition
	steps, err := i.workflowStepRepo.GetStepsByDefinitionID(ctx, def.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow steps: %w", err)
	}
	if len(steps) == 0 {
		return nil, fmt.Errorf("workflow definition has no steps registered")
	}

	// Schedule the first task
	firstStep := steps[0]
	resolvedQueue := firstStep.TaskQueue
	if resolvedQueue == "" {
		resolvedQueue = taskQueue
	}

	firstTask := &workflow.Task{
		ID:                  fmt.Sprintf("task-%s", uuid.New().String()),
		WorkflowExecutionID: exec.ID,
		StepIndex:           firstStep.StepIndex,
		StepName:            firstStep.StepName,
		TaskQueue:           resolvedQueue,
		Input:               input,
		State:               workflow.StateCreated,
		Attempt:             1,
		MaxAttempts:         3,
		ScheduledAt:         time.Now(),
	}

	if err := i.taskRepo.Create(ctx, firstTask); err != nil {
		return nil, fmt.Errorf("failed to schedule first task: %w", err)
	}

	// Record history events
	_ = i.historyRepo.Append(ctx, &workflow.HistoryEvent{
		WorkflowExecutionID: exec.ID,
		EventType:           "WorkflowExecutionStarted",
		Payload:             input,
		CreatedAt:           time.Now(),
	})

	_ = i.historyRepo.Append(ctx, &workflow.HistoryEvent{
		WorkflowExecutionID: exec.ID,
		StepIndex:           &firstStep.StepIndex,
		StepName:            &firstStep.StepName,
		EventType:           "StepScheduled",
		CreatedAt:           time.Now(),
	})

	i.taskBroker.Notify(resolvedQueue)

	return exec, nil
}

func (i *workflowInteractor) GetWorkflowResult(ctx context.Context, workflowID string) (*workflow.WorkflowExecution, error) {
	return i.executionRepo.GetByID(ctx, workflowID)
}

func (i *workflowInteractor) CancelWorkflow(ctx context.Context, workflowID string) error {
	exec, err := i.executionRepo.GetByID(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("failed to find execution: %w", err)
	}

	if workflow.IsTerminal(exec.State) {
		return fmt.Errorf("cannot cancel workflow in terminal state: %s", exec.State)
	}

	if err := i.executionRepo.UpdateState(ctx, workflowID, workflow.StateCancelled); err != nil {
		return fmt.Errorf("failed to cancel execution: %w", err)
	}

	_ = i.historyRepo.Append(ctx, &workflow.HistoryEvent{
		WorkflowExecutionID: workflowID,
		EventType:           "WorkflowExecutionCancelled",
		CreatedAt:           time.Now(),
	})

	return nil
}

func (i *workflowInteractor) GetWorkflowNameForExecution(ctx context.Context, executionID string) (string, error) {
	exec, err := i.executionRepo.GetByID(ctx, executionID)
	if err != nil {
		return "", err
	}
	return exec.WorkflowName, nil
}
