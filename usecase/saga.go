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

	// 3. Get all steps that have a compensation handler, up to failedAtStepIndex
	stepsToCompensate, err := i.workflowStepRepo.GetCompensationSteps(
		ctx, exec.WorkflowDefinitionID, failedAtStepIndex,
	)
	if err != nil {
		return fmt.Errorf("failed to get compensation steps: %w", err)
	}

	// 4. Get completed step outputs from history
	stepOutputs, err := i.historyRepo.GetStepOutputs(ctx, exec.ID)
	if err != nil {
		return fmt.Errorf("failed to get completed step outputs: %w", err)
	}

	// Build a map of stepIndex -> output for fast lookup and filter stepsToCompensate
	// to only include steps that actually succeeded (produced outputs)
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

	// 5. Record how many compensation tasks we're about to schedule
	if err := i.executionRepo.SetCompensationTotal(ctx, exec.ID, len(activeCompSteps)); err != nil {
		return err
	}

	if len(activeCompSteps) == 0 {
		// Nothing to compensate! Rollback is successfully complete.
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

	// 6. Schedule the first compensation task in serial reverse-order (the highest step_index)
	firstComp := activeCompSteps[0]
	ct := &workflow.CompensationTask{
		ID:                   uuid.New().String(),
		WorkflowExecutionID:  exec.ID,
		StepIndex:            firstComp.StepIndex,
		StepName:             firstComp.StepName,
		CompensationStepName: firstComp.CompensationStepName,
		TaskQueue:            exec.TaskQueue,
		Input:                outputByStep[firstComp.StepIndex], // original step output
		State:                workflow.StateCreated,
		MaxAttempts:          3,
		ScheduledAt:          time.Now(),
	}

	if err := i.compensationTaskRepo.Create(ctx, ct); err != nil {
		return fmt.Errorf("failed to schedule compensation task: %w", err)
	}

	_ = i.historyRepo.Append(ctx, &workflow.HistoryEvent{
		WorkflowExecutionID: exec.ID,
		StepIndex:           &ct.StepIndex,
		StepName:            &ct.StepName,
		EventType:           workflow.EventStepScheduled,
		CreatedAt:           time.Now(),
	})

	i.broker.Notify(exec.TaskQueue)
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

	if errString != "" {
		if t.Attempt < t.MaxAttempts {
			// Retry compensation task
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
			i.broker.Notify(t.TaskQueue)
			return nil
		}

		// Permanent failure of compensation task
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

	// Compensation task succeeded! Write COMPENSATION_COMPLETED event
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

	if newDone >= exec.CompensationTotal {
		// Saga rollback completely done! Mark execution FAILED.
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
		return nil
	}

	// Schedule the NEXT compensation task (highest index < t.StepIndex)
	stepsToCompensate, err := i.workflowStepRepo.GetCompensationSteps(
		ctx, exec.WorkflowDefinitionID, t.StepIndex-1,
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

	var nextComp *workflow.WorkflowDefinitionStep
	for _, step := range stepsToCompensate {
		if _, ok := outputByStep[step.StepIndex]; ok {
			nextComp = &step
			break
		}
	}

	if nextComp == nil {
		// No more steps need compensation
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
		return nil
	}

	ct := &workflow.CompensationTask{
		ID:                   uuid.New().String(),
		WorkflowExecutionID:  exec.ID,
		StepIndex:            nextComp.StepIndex,
		StepName:             nextComp.StepName,
		CompensationStepName: nextComp.CompensationStepName,
		TaskQueue:            exec.TaskQueue,
		Input:                outputByStep[nextComp.StepIndex],
		State:                workflow.StateCreated,
		MaxAttempts:          3,
		ScheduledAt:          time.Now(),
	}

	if err := i.compensationTaskRepo.Create(ctx, ct); err != nil {
		return fmt.Errorf("failed to schedule next compensation task: %w", err)
	}

	_ = i.historyRepo.Append(ctx, &workflow.HistoryEvent{
		WorkflowExecutionID: exec.ID,
		StepIndex:           &ct.StepIndex,
		StepName:            &ct.StepName,
		EventType:           workflow.EventStepScheduled,
		CreatedAt:           time.Now(),
	})

	i.broker.Notify(exec.TaskQueue)
	return nil
}
