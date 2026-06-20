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
	// 1. Find namespace by name
	ns, err := i.namespaceRepo.GetByName(ctx, workflowName)
	if err != nil {
		return nil, fmt.Errorf("namespace %q not found: %w", workflowName, workflow.ErrNotFound)
	}

	// 2. Find the active workflow definition
	def, err := i.workflowDefRepo.GetByName(ctx, ns.ID, workflowName)
	if err != nil {
		return nil, fmt.Errorf("workflow definition %q not found: %w", workflowName, err)
	}

	// 3. Create the execution
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

	// 4. Write WORKFLOW_STARTED history
	_ = i.historyRepo.Append(ctx, &workflow.HistoryEvent{
		WorkflowExecutionID: exec.ID,
		EventType:           workflow.EventWorkflowStarted,
		Payload:             input,
		CreatedAt:           time.Now(),
	})

	// 5. Transition to RUNNING. CREATED -> RUNNING is always a valid first
	// transition (see validTransitions in statemachine.go), so this can't
	// fail on the state-machine check, only on the underlying UpdateState
	// call itself.
	if err := workflow.ValidateTransition(exec.State, workflow.StateRunning); err != nil {
		return nil, fmt.Errorf("failed to start execution: %w", err)
	}
	if err := i.executionRepo.UpdateState(ctx, exec.ID, workflow.StateRunning); err != nil {
		return nil, err
	}
	exec.State = workflow.StateRunning

	// 6. Get step definitions
	steps, err := i.workflowStepRepo.GetStepsByDefinitionID(ctx, def.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get steps: %w", err)
	}

	if def.WorkflowType == workflow.IndependentWorkflow {
		// Schedule ALL steps immediately
		for _, step := range steps {
			t := buildTask(exec, &step, input, taskQueue)
			if err := i.taskRepo.Create(ctx, t); err != nil {
				return nil, fmt.Errorf("failed to schedule step %s: %w", step.StepName, err)
			}
			i.broker.Notify(taskQueue)
		}
	} else {
		// CHAIN: schedule only step 0 (index 1)
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
		Attempt:             1,
		MaxAttempts:         3,
		ScheduledAt:         time.Now(),
	}
}

func (i *workflowInteractor) GetWorkflowResult(ctx context.Context, workflowID string) (*workflow.WorkflowExecution, error) {
	return i.executionRepo.GetByID(ctx, workflowID)
}

// CancelWorkflow moves an execution to CANCELLED and stops any further work
// on it. Two things have to happen together for this to be correct:
//
//  1. The execution row transitions through ValidateTransition, so cancelling
//     an execution that's already terminal (COMPLETED/FAILED/CANCELLED)
//     returns ErrInvalidStateTransition instead of silently "succeeding" —
//     IsTerminal executions have no outgoing edges in validTransitions.
//  2. Any task row still CREATED/SCHEDULED/RUNNING for this execution is
//     marked CANCELLED via taskRepo.CancelByExecution. Without this, a
//     worker already holding the task (or one that picks it up moments
//     later) would call CompleteTask after the workflow is cancelled, and
//     CompleteTask would try to advance/complete an execution that no
//     longer exists in a live state. CompleteTask additionally re-checks
//     execution state itself (see task.go) as a second line of defense for
//     the case where a worker is already mid-flight when this runs.
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

	// Stop any pending/in-flight tasks from being picked up or acted on.
	// Best-effort is not appropriate here — if this fails, a worker could
	// still complete a task against a cancelled execution, so the cancel
	// itself should report failure rather than silently leaving tasks live.
	if err := i.taskRepo.CancelByExecution(ctx, workflowID); err != nil {
		return fmt.Errorf("failed to cancel pending tasks: %w", err)
	}

	_ = i.historyRepo.Append(ctx, &workflow.HistoryEvent{
		WorkflowExecutionID: workflowID,
		EventType:           workflow.EventWorkflowCancelled,
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