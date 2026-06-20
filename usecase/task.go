package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (i *workflowInteractor) PollTask(ctx context.Context, taskQueue string) (*workflow.Task, error) {
	return i.taskRepo.FindAndLockPending(ctx, taskQueue)
}

func (i *workflowInteractor) CompleteTask(ctx context.Context, taskID string, result []byte, errString string) error {
	if err := i.taskRepo.UpdateCompleted(ctx, taskID, result, errString); err != nil {
		return fmt.Errorf("failed to complete task: %w", err)
	}

	t, err := i.taskRepo.GetByID(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to find task: %w", err)
	}

	exec, err := i.executionRepo.GetByID(ctx, t.WorkflowExecutionID)
	if err != nil {
		return fmt.Errorf("failed to find execution: %w", err)
	}

	// Guard against acting on a task whose execution has already reached a
	// terminal state — most commonly because CancelWorkflow ran while this
	// task was in flight on a worker. taskRepo.CancelByExecution stops *new*
	// pickups, but a worker that already had the task locked before
	// cancellation will still call CompleteTask once it finishes; without
	// this check that call would try to advance a CHAIN cursor or tally an
	// INDEPENDENT step count against an execution that's already CANCELLED,
	// FAILED, or COMPLETED. The UpdateCompleted call above is left as-is
	// (the task row itself recording what actually happened is still
	// correct/useful for audit), but every state-mutating effect below is
	// skipped.
	if workflow.IsTerminal(exec.State) {
		log.Printf("info: ignoring CompleteTask for task %s: execution %s already terminal (%s)", t.ID, exec.ID, exec.State)
		return nil
	}

	if errString != "" {
		if t.Attempt < t.MaxAttempts {
			if err := i.historyRepo.Append(ctx, &workflow.HistoryEvent{
				WorkflowExecutionID: exec.ID,
				StepIndex:           &t.StepIndex,
				StepName:            &t.StepName,
				EventType:           workflow.EventStepRetrying,
				Error:               errString,
				CreatedAt:           time.Now(),
			}); err != nil {
				log.Printf("warn: failed to save history event: %v", err)
			}
			// Delete the original task row to avoid unique constraint conflict!
			_ = i.taskRepo.Delete(ctx, t.ID)

			retryTask := &workflow.Task{
				ID:                  uuid.New().String(),
				WorkflowExecutionID: exec.ID,
				StepIndex:           t.StepIndex,
				StepName:            t.StepName,
				TaskQueue:           t.TaskQueue,
				Input:               t.Input,
				State:               workflow.StateCreated,
				Attempt:             t.Attempt,
				MaxAttempts:         t.MaxAttempts,
				ScheduledAt:         time.Now().Add(time.Duration(t.Attempt*t.Attempt) * 5 * time.Second), // exponential backoff
			}
			if err := i.taskRepo.Create(ctx, retryTask); err != nil {
				return fmt.Errorf("failed to create retry task: %w", err)
			}
			i.broker.Notify(t.TaskQueue)
			return nil
		}

		if exec.WorkflowType == workflow.SagaWorkflow {
			_ = i.historyRepo.Append(ctx, &workflow.HistoryEvent{
				WorkflowExecutionID: exec.ID,
				StepIndex:           &t.StepIndex,
				StepName:            &t.StepName,
				EventType:           workflow.EventStepFailed,
				Error:               errString,
				CreatedAt:           time.Now(),
			})

			if err := i.startCompensation(ctx, exec, t.StepIndex-1); err != nil {
				log.Printf("error: failed to start saga compensation: %v", err)
				_ = i.executionRepo.UpdateState(ctx, exec.ID, workflow.StateFailed)
				_ = i.historyRepo.Append(ctx, &workflow.HistoryEvent{
					WorkflowExecutionID: exec.ID,
					EventType:           workflow.EventWorkflowFailed,
					Error:               fmt.Sprintf("Saga compensation start failed: %v", err),
					CreatedAt:           time.Now(),
				})
			}
			_ = i.taskRepo.Delete(ctx, t.ID)
			return nil
		}

		if err := workflow.ValidateTransition(exec.State, workflow.StateFailed); err != nil {
			return fmt.Errorf("cannot mark execution failed: %w", err)
		}
		if err := i.executionRepo.UpdateState(ctx, exec.ID, workflow.StateFailed); err != nil {
			return fmt.Errorf("failed to mark execution failed: %w", err)
		}
		if err := i.historyRepo.Append(ctx, &workflow.HistoryEvent{
			WorkflowExecutionID: exec.ID,
			StepIndex:           &t.StepIndex,
			StepName:            &t.StepName,
			EventType:           workflow.EventStepFailed,
			Error:               errString,
			CreatedAt:           time.Now(),
		}); err != nil {
			log.Printf("warn: failed to save history event: %v", err)
		}

		if err := i.historyRepo.Append(ctx, &workflow.HistoryEvent{
			WorkflowExecutionID: exec.ID,
			EventType:           workflow.EventWorkflowFailed,
			Error:               errString,
			CreatedAt:           time.Now(),
		}); err != nil {
			log.Printf("warn: failed to save history event: %v", err)
		}

		// Delete the task row after permanent failure
		_ = i.taskRepo.Delete(ctx, t.ID)
		return nil
	}

	if exec.WorkflowType == workflow.IndependentWorkflow {
		// 1. Write StepCompleted history (best-effort)
		if err := i.historyRepo.Append(ctx, &workflow.HistoryEvent{
			WorkflowExecutionID: exec.ID,
			StepIndex:           &t.StepIndex,
			StepName:            &t.StepName,
			EventType:           workflow.EventStepCompleted,
			Payload:             result,
			CreatedAt:           time.Now(),
		}); err != nil {
			log.Printf("warn: failed to save history event: %v", err)
		}

		
		newCount, err := i.executionRepo.IncrementCompletedSteps(ctx, exec.ID)
		if err != nil {
			return fmt.Errorf("failed to increment completed steps: %w", err)
		}

		if newCount == exec.TotalSteps {
			
			outputs, err := i.historyRepo.GetStepOutputs(ctx, exec.ID)
			if err != nil {
				return fmt.Errorf("failed to get step outputs: %w", err)
			}

			combinedBytes, err := json.Marshal(outputs)
			if err != nil {
				return fmt.Errorf("failed to marshal combined outputs: %w", err)
			}

			if err := workflow.ValidateTransition(exec.State, workflow.StateCompleted); err != nil {
				return fmt.Errorf("cannot mark execution complete: %w", err)
			}
			if err := i.executionRepo.UpdateState(ctx, exec.ID, workflow.StateCompleted); err != nil {
				return fmt.Errorf("failed to mark execution complete: %w", err)
			}
			if err := i.executionRepo.SaveResult(ctx, exec.ID, combinedBytes); err != nil {
				return fmt.Errorf("failed to save execution result: %w", err)
			}
			if err := i.historyRepo.Append(ctx, &workflow.HistoryEvent{
				WorkflowExecutionID: exec.ID,
				EventType:           workflow.EventWorkflowCompleted,
				Payload:             combinedBytes,
				CreatedAt:           time.Now(),
			}); err != nil {
				log.Printf("warn: failed to save history event: %v", err)
			}
		}

		// Completed task - delete the task row
		_ = i.taskRepo.Delete(ctx, t.ID)
		return nil
	}

	// CHAIN workflow behavior
	if err := i.checkpointRepo.Save(ctx, &workflow.Checkpoint{
		WorkflowExecutionID: exec.ID,
		StepIndex:           t.StepIndex,
		StateSnapshot:       result,
		CreatedAt:           time.Now(),
	}); err != nil {
		return fmt.Errorf("failed to save checkpoint at step %d: %w", t.StepIndex, err)
	}

	// Write StepCompleted history (best-effort)
	if err := i.historyRepo.Append(ctx, &workflow.HistoryEvent{
		WorkflowExecutionID: exec.ID,
		StepIndex:           &t.StepIndex,
		StepName:            &t.StepName,
		EventType:           workflow.EventStepCompleted,
		Payload:             result,
		CreatedAt:           time.Now(),
	}); err != nil {
		log.Printf("warn: failed to save history event: %v", err)
	}

	nextIndex := t.StepIndex + 1

	if nextIndex > exec.TotalSteps {
		// All steps done
		if err := workflow.ValidateTransition(exec.State, workflow.StateCompleted); err != nil {
			return fmt.Errorf("cannot mark execution complete: %w", err)
		}
		if err := i.executionRepo.UpdateState(ctx, exec.ID, workflow.StateCompleted); err != nil {
			return fmt.Errorf("failed to mark execution complete: %w", err)
		}
		if err := i.executionRepo.SaveResult(ctx, exec.ID, result); err != nil {
			return fmt.Errorf("failed to save execution result: %w", err)
		}
		if err := i.historyRepo.Append(ctx, &workflow.HistoryEvent{
			WorkflowExecutionID: exec.ID,
			EventType:           workflow.EventWorkflowCompleted,
			Payload:             result,
			CreatedAt:           time.Now(),
		}); err != nil {
			log.Printf("warn: failed to save history event: %v", err)
		}

		// Completed task - delete the task row
		_ = i.taskRepo.Delete(ctx, t.ID)
		return nil
	}

	// Fetch step definition using the new repo method
	nextStep, err := i.workflowStepRepo.GetByDefinitionAndIndex(ctx, exec.WorkflowDefinitionID, nextIndex)
	if err != nil {
		return fmt.Errorf("failed to find next step definition: %w", err)
	}

	resolvedQueue := nextStep.TaskQueue
	if resolvedQueue == "" {
		resolvedQueue = exec.TaskQueue
	}

	nextTask := &workflow.Task{
		ID:                  uuid.New().String(),
		WorkflowExecutionID: exec.ID,
		StepIndex:           nextIndex,
		StepName:            nextStep.StepName,
		TaskQueue:           resolvedQueue,
		Input:               result, // ← chain
		State:               workflow.StateCreated,
		Attempt:             1,
		MaxAttempts:         3,
		ScheduledAt:         time.Now(),
	}
	if err := i.taskRepo.Create(ctx, nextTask); err != nil {
		return fmt.Errorf("failed to save next task: %w", err)
	}
	i.broker.Notify(resolvedQueue)

	if err := i.executionRepo.UpdateStepCursor(ctx, exec.ID, nextIndex); err != nil {
		return fmt.Errorf("failed to advance step cursor: %w", err)
	}
	if err := i.historyRepo.Append(ctx, &workflow.HistoryEvent{
		WorkflowExecutionID: exec.ID,
		StepIndex:           &nextIndex,
		StepName:            &nextStep.StepName,
		EventType:           workflow.EventStepScheduled,
		CreatedAt:           time.Now(),
	}); err != nil {
		log.Printf("warn: failed to save history event: %v", err)
	}

	// Completed task - delete the task row
	_ = i.taskRepo.Delete(ctx, t.ID)
	return nil
}