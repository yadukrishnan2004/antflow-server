package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (i *workflowInteractor) StartWorkflow(
	ctx context.Context, workflowName string, taskQueue string, input []byte,
) (*workflow.WorkflowExecution, error) {

	ns, err := i.namespaceRepo.GetByName(ctx, workflowName)
	if err != nil {
		return nil, fmt.Errorf("namespace %q not found: %w", workflowName, workflow.ErrNotFound)
	}

	def, err := i.workflowDefRepo.GetByName(ctx, ns.ID, workflowName)
	if err != nil {
		return nil, fmt.Errorf("workflow definition %q not found: %w", workflowName, err)
	}

	exec := &workflow.WorkflowExecution{
		ID:                   uuid.New().String(),
		WorkflowDefinitionID: def.ID,
		WorkflowName:         def.Name,
		WorkflowType:         def.WorkflowType,
		TotalSteps:           def.Steps,
		TaskQueue:            taskQueue,
		Input:                input,
		State:                workflow.StateCreated,
		CurrentStep:          0,
		ScheduledAt:          time.Now(),
		CreatedAt:            time.Now(),
		UpdatedAt:            time.Now(),
	}
	if err := i.executionRepo.Create(ctx, exec); err != nil {
		return nil, fmt.Errorf("failed to create execution: %w", err)
	}

	_ = i.historyRepo.Append(ctx, &workflow.HistoryEvent{
		WorkflowExecutionID: exec.ID,
		EventType:           workflow.EventWorkflowStarted,
		Payload:             input,
		CreatedAt:           time.Now(),
	})

	if err := workflow.ValidateTransition(exec.State, workflow.StateRunning); err != nil {
		return nil, fmt.Errorf("failed to start execution: %w", err)
	}
	if err := i.executionRepo.UpdateState(ctx, exec.ID, workflow.StateRunning); err != nil {
		return nil, err
	}
	exec.State = workflow.StateRunning

	steps, err := i.workflowStepRepo.GetStepsByDefinitionID(ctx, def.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get steps: %w", err)
	}

	if def.WorkflowType == workflow.IndependentWorkflow {
		for _, step := range steps {
			t := buildTask(exec, &step, input, taskQueue)
			if err := i.taskRepo.Create(ctx, t); err != nil {
				return nil, fmt.Errorf("failed to schedule step %s: %w", step.StepName, err)
			}
			i.broker.Notify(taskQueue)
		}
	} else {
		if len(steps) == 0 {
			return nil, fmt.Errorf("workflow has no steps")
		}
		t := buildTask(exec, &steps[0], input, taskQueue)
		if err := i.taskRepo.Create(ctx, t); err != nil {
			return nil, fmt.Errorf("failed to schedule first step: %w", err)
		}
		i.broker.Notify(taskQueue)
	}

	return exec, nil
}

func buildTask(exec *workflow.WorkflowExecution, step *workflow.WorkflowDefinitionStep, input []byte, defaultQueue string) *workflow.Task {
	q := step.TaskQueue
	if q == "" {
		q = defaultQueue
	}
	return &workflow.Task{
		ID:                  uuid.New().String(),
		WorkflowExecutionID: exec.ID,
		StepIndex:           step.StepIndex,
		StepName:            step.StepName,
		TaskQueue:           q,
		Input:               input,
		State:               workflow.StateCreated,
		Attempt:             0,
		MaxAttempts:         3,
		ScheduledAt:         time.Now(),
	}
}

func (i *workflowInteractor) GetWorkflowResult(ctx context.Context, workflowID string) (*workflow.WorkflowExecution, error) {
	return i.executionRepo.GetByID(ctx, workflowID)
}

func (i *workflowInteractor) CancelWorkflow(ctx context.Context, workflowID string) error {
	exec, err := i.executionRepo.GetByID(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("failed to find execution: %w", err)
	}

	if err := workflow.ValidateTransition(exec.State, workflow.StateCancelled); err != nil {
		return fmt.Errorf("cannot cancel workflow: %w", err)
	}

	if err := i.executionRepo.UpdateState(ctx, workflowID, workflow.StateCancelled); err != nil {
		return fmt.Errorf("failed to cancel execution: %w", err)
	}

	if err := i.taskRepo.CancelByExecution(ctx, workflowID); err != nil {
		return fmt.Errorf("failed to cancel pending tasks: %w", err)
	}

	_ = i.historyRepo.Append(ctx, &workflow.HistoryEvent{
		WorkflowExecutionID: workflowID,
		EventType:           workflow.EventWorkflowCancelled,
		CreatedAt:           time.Now(),
	})

	// Drain any buffered signals and unblock any waiting step goroutines so
	// they don't leak. This must happen AFTER the state is persisted so a
	// racing WaitForSignal can see the terminal state on retry.
	i.signals.Drain(workflowID)

	return nil
}

func (i *workflowInteractor) GetWorkflowNameForExecution(ctx context.Context, executionID string) (string, error) {
	exec, err := i.executionRepo.GetByID(ctx, executionID)
	if err != nil {
		return "", err
	}
	return exec.WorkflowName, nil
}