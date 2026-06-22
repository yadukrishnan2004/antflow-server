package usecase

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (i *workflowInteractor) PollCompensationTask(ctx context.Context, taskQueue string) (*workflow.CompensationTask, error) {
	return i.compensationTaskRepo.FindAndLockPending(ctx, taskQueue)
}

func (i *workflowInteractor) startCompensation(
	ctx context.Context,
	exec *workflow.WorkflowExecution,
	failedAtStepIndex int,
) error {
	log.Printf("info: startCompensation exec=%s failedAtStepIndex=%d totalSteps=%d",
		exec.ID, failedAtStepIndex, exec.TotalSteps)

	// 1. Transition execution to COMPENSATING
	if err := workflow.ValidateTransition(exec.State, workflow.StateCompensating); err != nil {
		return err
	}
	if err := i.executionRepo.UpdateState(ctx, exec.ID, workflow.StateCompensating); err != nil {
		return err
	}

	// 2. Write COMPENSATION_STARTED event
	_ = i.historyRepo.Append(ctx, &workflow.HistoryEvent{
		WorkflowExecutionID: exec.ID,
		EventType:           workflow.EventCompensationStarted,
		CreatedAt:           time.Now(),
	})

	// 3. Get all steps that have a compensation handler, up to the step
	// BEFORE the failed one. The failed step never completed so it has no
	// output and nothing to undo — we only compensate steps that succeeded.
	stepsToCompensate, err := i.workflowStepRepo.GetCompensationSteps(
		ctx, exec.WorkflowDefinitionID, failedAtStepIndex-1,
	)
	if err != nil {
		return fmt.Errorf("failed to get compensation steps: %w", err)
	}

	log.Printf("info: startCompensation found %d candidate steps (up to index %d)",
		len(stepsToCompensate), failedAtStepIndex-1)

	// 4. Get completed step outputs from history — only steps that produced
	// a STEP_COMPLETED event are eligible for compensation.
	stepOutputs, err := i.historyRepo.GetStepOutputs(ctx, exec.ID)
	if err != nil {
		return fmt.Errorf("failed to get completed step outputs: %w", err)
	}

	log.Printf("info: startCompensation found %d step outputs in history", len(stepOutputs))

	// Build a map of stepIndex -> output for fast lookup, then filter to
	// only steps that actually succeeded (produced output).
	outputByStep := map[int][]byte{}
	for _, o := range stepOutputs {
		outputByStep[o.StepIndex] = o.Output
	}

	var activeCompSteps []workflow.WorkflowDefinitionStep
	for _, step := range stepsToCompensate {
		if _, ok := outputByStep[step.StepIndex]; ok {
			activeCompSteps = append(activeCompSteps, step)
		}
	}

	log.Printf("info: startCompensation will schedule %d compensation tasks", len(activeCompSteps))

	// 5. Record how many compensation tasks we're about to schedule
	if err := i.executionRepo.SetCompensationTotal(ctx, exec.ID, len(activeCompSteps)); err != nil {
		return err
	}

	if len(activeCompSteps) == 0 {
		// Nothing to compensate — no prior steps succeeded, so rollback is
		// trivially complete. Mark the execution FAILED and close it out.
		if err := i.executionRepo.UpdateState(ctx, exec.ID, workflow.StateFailed); err != nil {
			return err
		}
		_ = i.historyRepo.Append(ctx, &workflow.HistoryEvent{
			WorkflowExecutionID: exec.ID,
			EventType:           workflow.EventSagaRolledBack,
			CreatedAt:           time.Now(),
		})
		_ = i.historyRepo.Append(ctx, &workflow.HistoryEvent{
			WorkflowExecutionID: exec.ID,
			EventType:           workflow.EventWorkflowFailed,
			Error:               "Saga rolled back: no successful steps to compensate",
			CreatedAt:           time.Now(),
		})
		return nil
	}

	// 6. Schedule the FIRST compensation task. Steps are returned in reverse
	// order (highest step_index first) by GetCompensationSteps, so index [0]
	// is the most recently completed step — correct starting point.
	firstComp := activeCompSteps[0]
	q := firstComp.TaskQueue
	if q == "" {
		q = exec.TaskQueue
	}
	ct := &workflow.CompensationTask{
		ID:                   uuid.New().String(),
		WorkflowExecutionID:  exec.ID,
		StepIndex:            firstComp.StepIndex,
		StepName:             firstComp.StepName,
		CompensationStepName: firstComp.CompensationStepName,
		TaskQueue:            q,
		Input:                outputByStep[firstComp.StepIndex],
		State:                workflow.StateCreated,
		MaxAttempts:          3,
		ScheduledAt:          time.Now(),
	}

	if err := i.compensationTaskRepo.Create(ctx, ct); err != nil {
		return fmt.Errorf("failed to schedule compensation task: %w", err)
	}

	log.Printf("info: scheduled first compensation task id=%s stepIndex=%d stepName=%s compensationStep=%s",
		ct.ID, ct.StepIndex, ct.StepName, ct.CompensationStepName)

	_ = i.historyRepo.Append(ctx, &workflow.HistoryEvent{
		WorkflowExecutionID: exec.ID,
		StepIndex:           &ct.StepIndex,
		StepName:            &ct.StepName,
		EventType:           workflow.EventStepScheduled,
		CreatedAt:           time.Now(),
	})

	i.broker.Notify(q)
	return nil
}

func (i *workflowInteractor) CompleteCompensationTask(
	ctx context.Context,
	taskID string,
	result []byte,
	errString string,
) error {
	if err := i.compensationTaskRepo.UpdateCompleted(ctx, taskID, result, errString); err != nil {
		return fmt.Errorf("failed to complete task: %w", err)
	}

	t, err := i.compensationTaskRepo.GetByID(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to find compensation task: %w", err)
	}

	exec, err := i.executionRepo.GetByID(ctx, t.WorkflowExecutionID)
	if err != nil {
		return fmt.Errorf("failed to find execution: %w", err)
	}

	if workflow.IsTerminal(exec.State) {
		log.Printf("info: ignoring CompleteCompensationTask: execution %s already terminal (%s)", exec.ID, exec.State)
		return nil
	}

	// ── Compensation task FAILED ──────────────────────────────────────────
	if errString != "" {
		if t.Attempt < t.MaxAttempts {
			// Delete the failed row and re-create with the same step_index
			// so the unique constraint (workflow_execution_id, step_index)
			// doesn't block the insert.
			_ = i.compensationTaskRepo.Delete(ctx, t.ID)
			retryTask := &workflow.CompensationTask{
				ID:                   uuid.New().String(),
				WorkflowExecutionID:  exec.ID,
				StepIndex:            t.StepIndex,
				StepName:             t.StepName,
				CompensationStepName: t.CompensationStepName,
				TaskQueue:            t.TaskQueue,
				Input:                t.Input,
				State:                workflow.StateCreated,
				Attempt:              t.Attempt,
				MaxAttempts:          t.MaxAttempts,
				ScheduledAt:          time.Now().Add(time.Duration(t.Attempt*t.Attempt) * 5 * time.Second),
			}
			if err := i.compensationTaskRepo.Create(ctx, retryTask); err != nil {
				return fmt.Errorf("failed to create retry compensation task: %w", err)
			}
			log.Printf("info: retrying compensation task stepIndex=%d attempt=%d/%d",
				t.StepIndex, t.Attempt, t.MaxAttempts)
			i.broker.Notify(t.TaskQueue)
			return nil
		}

		// Permanent failure — the saga rollback itself failed.
		log.Printf("error: compensation task permanently failed stepIndex=%d err=%s", t.StepIndex, errString)

		_ = i.historyRepo.Append(ctx, &workflow.HistoryEvent{
			WorkflowExecutionID: exec.ID,
			StepIndex:           &t.StepIndex,
			StepName:            &t.StepName,
			EventType:           workflow.EventCompensationFailed,
			Error:               errString,
			CreatedAt:           time.Now(),
		})
		_ = i.historyRepo.Append(ctx, &workflow.HistoryEvent{
			WorkflowExecutionID: exec.ID,
			EventType:           workflow.EventSagaRollbackFailed,
			Error:               errString,
			CreatedAt:           time.Now(),
		})
		_ = i.historyRepo.Append(ctx, &workflow.HistoryEvent{
			WorkflowExecutionID: exec.ID,
			EventType:           workflow.EventWorkflowFailed,
			Error:               fmt.Sprintf("Saga rollback failed at step %s: %s", t.StepName, errString),
			CreatedAt:           time.Now(),
		})
		_ = i.executionRepo.UpdateState(ctx, exec.ID, workflow.StateFailed)
		_ = i.compensationTaskRepo.Delete(ctx, t.ID)
		return nil
	}

	// ── Compensation task SUCCEEDED ───────────────────────────────────────
	log.Printf("info: compensation task succeeded stepIndex=%d stepName=%s", t.StepIndex, t.StepName)

	_ = i.historyRepo.Append(ctx, &workflow.HistoryEvent{
		WorkflowExecutionID: exec.ID,
		StepIndex:           &t.StepIndex,
		StepName:            &t.StepName,
		EventType:           workflow.EventCompensationCompleted,
		Payload:             result,
		CreatedAt:           time.Now(),
	})

	newDone, err := i.executionRepo.IncrementCompensationDone(ctx, exec.ID)
	if err != nil {
		return fmt.Errorf("failed to increment compensation done: %w", err)
	}

	_ = i.compensationTaskRepo.Delete(ctx, t.ID)

	// Re-fetch the execution so compensation_total is authoritative — the
	// value in the local `exec` variable was read before any concurrent
	// writes to that column could have settled.
	freshExec, err := i.executionRepo.GetByID(ctx, exec.ID)
	if err != nil {
		return fmt.Errorf("failed to re-fetch execution after compensation done: %w", err)
	}

	log.Printf("info: compensation progress done=%d total=%d", newDone, freshExec.CompensationTotal)

	if newDone >= freshExec.CompensationTotal {
		// All compensation tasks finished — saga rollback is complete.
		// The overall workflow still ends as FAILED (the original step did
		// fail); we just rolled back cleanly.
		if err := i.executionRepo.UpdateState(ctx, exec.ID, workflow.StateFailed); err != nil {
			return err
		}
		_ = i.historyRepo.Append(ctx, &workflow.HistoryEvent{
			WorkflowExecutionID: exec.ID,
			EventType:           workflow.EventSagaRolledBack,
			CreatedAt:           time.Now(),
		})
		_ = i.historyRepo.Append(ctx, &workflow.HistoryEvent{
			WorkflowExecutionID: exec.ID,
			EventType:           workflow.EventWorkflowFailed,
			Error:               "Saga rolled back successfully",
			CreatedAt:           time.Now(),
		})
		log.Printf("info: saga rollback complete for exec=%s", exec.ID)
		return nil
	}

	// ── Schedule the NEXT compensation task ───────────────────────────────
	// Walk backwards: find the highest step_index below t.StepIndex that
	// has a compensation handler AND a recorded output (i.e. it succeeded).
	stepsToCompensate, err := i.workflowStepRepo.GetCompensationSteps(
		ctx, freshExec.WorkflowDefinitionID, t.StepIndex-1,
	)
	if err != nil {
		return fmt.Errorf("failed to get next compensation steps: %w", err)
	}

	stepOutputs, err := i.historyRepo.GetStepOutputs(ctx, exec.ID)
	if err != nil {
		return fmt.Errorf("failed to get completed step outputs: %w", err)
	}

	outputByStep := map[int][]byte{}
	for _, o := range stepOutputs {
		outputByStep[o.StepIndex] = o.Output
	}

	// stepsToCompensate is ordered DESC by step_index, so the first match
	// is the next step to roll back.
	var nextComp *workflow.WorkflowDefinitionStep
	for idx := range stepsToCompensate {
		step := &stepsToCompensate[idx] // take address of slice element, not loop copy
		if _, ok := outputByStep[step.StepIndex]; ok {
			nextComp = step
			break
		}
	}

	if nextComp == nil {
		// No more steps need rolling back — we're done.
		if err := i.executionRepo.UpdateState(ctx, exec.ID, workflow.StateFailed); err != nil {
			return err
		}
		_ = i.historyRepo.Append(ctx, &workflow.HistoryEvent{
			WorkflowExecutionID: exec.ID,
			EventType:           workflow.EventSagaRolledBack,
			CreatedAt:           time.Now(),
		})
		_ = i.historyRepo.Append(ctx, &workflow.HistoryEvent{
			WorkflowExecutionID: exec.ID,
			EventType:           workflow.EventWorkflowFailed,
			Error:               "Saga rolled back successfully",
			CreatedAt:           time.Now(),
		})
		log.Printf("info: saga rollback complete (no more steps) for exec=%s", exec.ID)
		return nil
	}

	q := nextComp.TaskQueue
	if q == "" {
		q = freshExec.TaskQueue
	}
	ct := &workflow.CompensationTask{
		ID:                   uuid.New().String(),
		WorkflowExecutionID:  exec.ID,
		StepIndex:            nextComp.StepIndex,
		StepName:             nextComp.StepName,
		CompensationStepName: nextComp.CompensationStepName,
		TaskQueue:            q,
		Input:                outputByStep[nextComp.StepIndex],
		State:                workflow.StateCreated,
		MaxAttempts:          3,
		ScheduledAt:          time.Now(),
	}

	if err := i.compensationTaskRepo.Create(ctx, ct); err != nil {
		return fmt.Errorf("failed to schedule next compensation task: %w", err)
	}

	log.Printf("info: scheduled next compensation task id=%s stepIndex=%d stepName=%s compensationStep=%s",
		ct.ID, ct.StepIndex, ct.StepName, ct.CompensationStepName)

	_ = i.historyRepo.Append(ctx, &workflow.HistoryEvent{
		WorkflowExecutionID: exec.ID,
		StepIndex:           &ct.StepIndex,
		StepName:            &ct.StepName,
		EventType:           workflow.EventStepScheduled,
		CreatedAt:           time.Now(),
	})

	i.broker.Notify(q)
	return nil
}