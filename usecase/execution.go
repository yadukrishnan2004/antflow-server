package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)


func (i *workflowInteractor) StartWorkflow(
	ctx context.Context, workflowDefID string, taskQueue string, input []byte, timeoutOverrideSeconds int,
) (*workflow.WorkflowExecution, error) {

	def, err := i.workflowDefRepo.GetByID(ctx, workflowDefID)
	if err != nil {
		return nil, fmt.Errorf("workflow definition ID %q not found: %w", workflowDefID, err)
	}

	effectiveTimeout := def.DefaultTimeoutSeconds
	if timeoutOverrideSeconds > 0 {
		effectiveTimeout = timeoutOverrideSeconds
	}

	var deadline *time.Time
	if effectiveTimeout > 0 {
		d := time.Now().Add(time.Duration(effectiveTimeout) * time.Second)
		deadline = &d
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
		DeadlineAt:           deadline,
		ScheduledAt:          time.Now(),
		CreatedAt:            time.Now(),
		UpdatedAt:            time.Now(),
	}

	err = i.txManager.RunInTx(ctx, func(txCtx context.Context) error {
		if err := i.executionRepo.Create(txCtx, exec); err != nil {
			return fmt.Errorf("failed to create execution: %w", err)
		}

		_ = i.historyRepo.Append(txCtx, &workflow.HistoryEvent{
			WorkflowExecutionID: exec.ID,
			EventType:           workflow.EventWorkflowStarted,
			Payload:             input,
			CreatedAt:           time.Now(),
		})

		if err := workflow.ValidateTransition(exec.State, workflow.StateRunning); err != nil {
			return fmt.Errorf("failed to start execution: %w", err)
		}
		if err := i.executionRepo.UpdateState(txCtx, exec.ID, workflow.StateRunning); err != nil {
			return err
		}
		exec.State = workflow.StateRunning

		steps, err := i.workflowStepRepo.GetStepsByDefinitionID(txCtx, def.ID)
		if err != nil {
			return fmt.Errorf("failed to get steps: %w", err)
		}

		if def.WorkflowType == workflow.IndependentWorkflow {
			for _, step := range steps {
				t := buildTask(exec, &step, input, taskQueue)
				if err := i.taskRepo.Create(txCtx, t); err != nil {
					return fmt.Errorf("failed to schedule step %s: %w", step.StepName, err)
				}
			}
		} else {
			if len(steps) == 0 {
				return fmt.Errorf("workflow has no steps")
			}
			t := buildTask(exec, &steps[0], input, taskQueue)
			if err := i.taskRepo.Create(txCtx, t); err != nil {
				return fmt.Errorf("failed to schedule first step: %w", err)
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	// Trigger broker notification outside transaction so it triggers immediately
	i.broker.Notify(taskQueue)
	return exec, nil
}

func (i *workflowInteractor) CancelWorkflow(ctx context.Context, workflowID string) error {
	exec, err := i.executionRepo.GetByID(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("failed to find execution: %w", err)
	}

	if err := workflow.ValidateTransition(exec.State, workflow.StateCancelled); err != nil {
		return fmt.Errorf("cannot cancel workflow: %w", err)
	}

	err = i.txManager.RunInTx(ctx, func(txCtx context.Context) error {
		if err := i.executionRepo.UpdateState(txCtx, workflowID, workflow.StateCancelled); err != nil {
			return fmt.Errorf("failed to cancel execution: %w", err)
		}

		if err := i.taskRepo.CancelByExecution(txCtx, workflowID); err != nil {
			return fmt.Errorf("failed to cancel pending tasks: %w", err)
		}

		if err := i.compensationTaskRepo.CancelByExecution(txCtx, workflowID); err != nil {
			return fmt.Errorf("failed to cancel pending compensation tasks: %w", err)
		}

		_ = i.historyRepo.Append(txCtx, &workflow.HistoryEvent{
			WorkflowExecutionID: workflowID,
			EventType:           workflow.EventWorkflowCancelled,
			CreatedAt:           time.Now(),
		})
		return nil
	})

	if err != nil {
		return err
	}

	i.signals.Drain(workflowID)
	return nil
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
        TimeoutSeconds:      step.TimeoutSeconds,
    }
}

func (i *workflowInteractor) GetWorkflowResult(ctx context.Context, workflowID string) (*workflow.WorkflowExecution, error) {
	return i.executionRepo.GetByID(ctx, workflowID)
}


func (i *workflowInteractor) GetWorkflowNameForExecution(ctx context.Context, executionID string) (string, error) {
	exec, err := i.executionRepo.GetByID(ctx, executionID)
	if err != nil {
		return "", err
	}
	return exec.WorkflowName, nil
}

func (i *workflowInteractor) GetWorkflowIdForExecution(ctx context.Context, workflowName string) (string, error) {
    ns, err := i.namespaceRepo.GetByName(ctx, workflowName)
    if err != nil {
        return "", err
    }
    def, err := i.workflowDefRepo.GetByName(ctx, ns.ID, workflowName)
    if err != nil {
        return "", err
    }
    return def.ID, nil
}