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
	var notifyQueue string

	err := i.txManager.RunInTx(ctx, func(txCtx context.Context) error {
		if err := i.taskRepo.UpdateCompleted(txCtx, taskID, result, errString); err != nil {
			return fmt.Errorf("failed to complete task: %w", err)
		}

		t, err := i.taskRepo.GetByID(txCtx, taskID)
		if err != nil {
			return fmt.Errorf("failed to find task: %w", err)
		}

		exec, err := i.executionRepo.GetByID(txCtx, t.WorkflowExecutionID)
		if err != nil {
			return fmt.Errorf("failed to find execution: %w", err)
		}

		// Guard against acting on a task whose execution has already reached a
		// terminal state.
		if workflow.IsTerminal(exec.State) {
			log.Printf("info: ignoring CompleteTask for task %s: execution %s already terminal (%s)", t.ID, exec.ID, exec.State)
			return nil
		}

		// ── Task FAILED ───────────────────────────────────────────────────────
		if errString != "" {
			if t.Attempt < t.MaxAttempts {
				if err := i.historyRepo.Append(txCtx, &workflow.HistoryEvent{
					WorkflowExecutionID: exec.ID,
					StepIndex:           &t.StepIndex,
					StepName:            &t.StepName,
					EventType:           workflow.EventStepRetrying,
					Error:               errString,
					CreatedAt:           time.Now(),
				}); err != nil {
					log.Printf("warn: failed to save history event: %v", err)
				}
				_ = i.taskRepo.Delete(txCtx, t.ID)

				retryTask := &workflow.Task{
					ID:                  uuid.New().String(),
					WorkflowExecutionID: exec.ID,
					StepIndex:           t.StepIndex,
					StepName:            t.StepName,
					TaskQueue:           t.TaskQueue,
					Input:               t.Input,
					State:               workflow.StateCreated,
					Attempt:             t.Attempt + 1,
					MaxAttempts:         t.MaxAttempts,
					ScheduledAt:         time.Now().Add(time.Duration((t.Attempt+1)*(t.Attempt+1)) * 5 * time.Second),
					TimeoutSeconds:      t.TimeoutSeconds, 
				}
				if err := i.taskRepo.Create(txCtx, retryTask); err != nil {
					return fmt.Errorf("failed to create retry task: %w", err)
				}
				
				// Queue broker notification for after transaction commits
				notifyQueue = t.TaskQueue
				return nil
			}

			// Permanent failure.
			log.Printf("info: task %s permanently failed at stepIndex=%d stepName=%s workflowType=%s",
				t.ID, t.StepIndex, t.StepName, exec.WorkflowType)

			if exec.WorkflowType == workflow.SagaWorkflow {
				_ = i.historyRepo.Append(txCtx, &workflow.HistoryEvent{
					WorkflowExecutionID: exec.ID,
					StepIndex:           &t.StepIndex,
					StepName:            &t.StepName,
					EventType:           workflow.EventStepFailed,
					Error:               errString,
					CreatedAt:           time.Now(),
				})

							if err := i.startCompensation(txCtx, exec, t.StepIndex); err != nil {
				log.Printf("error: failed to start saga compensation: %v", err)
				_ = i.executionRepo.SaveError(txCtx, exec.ID, fmt.Sprintf("Saga compensation start failed: %v", err))
				_ = i.executionRepo.UpdateState(txCtx, exec.ID, workflow.StateFailed)
				_ = i.historyRepo.Append(txCtx, &workflow.HistoryEvent{
					WorkflowExecutionID: exec.ID,
					EventType:           workflow.EventWorkflowFailed,
					Error:               fmt.Sprintf("Saga compensation start failed: %v", err),
					CreatedAt:           time.Now(),
				})
				i.signals.Drain(exec.ID)
			}
			_ = i.taskRepo.Delete(txCtx, t.ID)
			return nil
			}

		// Non-saga workflow: mark execution FAILED immediately.
		if err := workflow.ValidateTransition(exec.State, workflow.StateFailed); err != nil {
			return fmt.Errorf("cannot mark execution failed: %w", err)
		}
		// Save error message to database first
		if err := i.executionRepo.SaveError(txCtx, exec.ID, errString); err != nil {
			return fmt.Errorf("failed to save execution error: %w", err)
		}
		if err := i.executionRepo.UpdateState(txCtx, exec.ID, workflow.StateFailed); err != nil {
			return fmt.Errorf("failed to mark execution failed: %w", err)
		}
			if err := i.taskRepo.CancelByExecution(txCtx, exec.ID); err != nil {
				log.Printf("warn: failed to cancel remaining tasks for failed execution %s: %v", exec.ID, err)
			}

			if err := i.historyRepo.Append(txCtx, &workflow.HistoryEvent{
				WorkflowExecutionID: exec.ID,
				StepIndex:           &t.StepIndex,
				StepName:            &t.StepName,
				EventType:           workflow.EventStepFailed,
				Error:               errString,
				CreatedAt:           time.Now(),
			}); err != nil {
				log.Printf("warn: failed to save history event: %v", err)
			}
			if err := i.historyRepo.Append(txCtx, &workflow.HistoryEvent{
				WorkflowExecutionID: exec.ID,
				EventType:           workflow.EventWorkflowFailed,
				Error:               errString,
				CreatedAt:           time.Now(),
			}); err != nil {
				log.Printf("warn: failed to save history event: %v", err)
			}
			_ = i.taskRepo.Delete(txCtx, t.ID)

			// Drain any buffered signals and unblock waiting step goroutines.
			i.signals.Drain(exec.ID)
			return nil
		}

		// ── Task SUCCEEDED ────────────────────────────────────────────────────

		if exec.WorkflowType == workflow.IndependentWorkflow {
			if err := i.historyRepo.Append(txCtx, &workflow.HistoryEvent{
				WorkflowExecutionID: exec.ID,
				StepIndex:           &t.StepIndex,
				StepName:            &t.StepName,
				EventType:           workflow.EventStepCompleted,
				Payload:             result,
				CreatedAt:           time.Now(),
			}); err != nil {
				log.Printf("warn: failed to save history event: %v", err)
			}

			newCount, err := i.executionRepo.IncrementCompletedSteps(txCtx, exec.ID)
			if err != nil {
				return fmt.Errorf("failed to increment completed steps: %w", err)
			}

			if newCount == exec.TotalSteps {
				outputs, err := i.historyRepo.GetStepOutputs(txCtx, exec.ID)
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
				if err := i.executionRepo.UpdateState(txCtx, exec.ID, workflow.StateCompleted); err != nil {
					return fmt.Errorf("failed to mark execution complete: %w", err)
				}
				if err := i.executionRepo.SaveResult(txCtx, exec.ID, combinedBytes); err != nil {
					return fmt.Errorf("failed to save execution result: %w", err)
				}
				if err := i.historyRepo.Append(txCtx, &workflow.HistoryEvent{
					WorkflowExecutionID: exec.ID,
					EventType:           workflow.EventWorkflowCompleted,
					Payload:             combinedBytes,
					CreatedAt:           time.Now(),
				}); err != nil {
					log.Printf("warn: failed to save history event: %v", err)
				}
				// Drain signals now that the execution is terminal.
				i.signals.Drain(exec.ID)
			}

			_ = i.taskRepo.Delete(txCtx, t.ID)
			return nil
		}

		// ── CHAIN (and SAGA success path) ─────────────────────────────────────
		if err := i.checkpointRepo.Save(txCtx, &workflow.Checkpoint{
			WorkflowExecutionID: exec.ID,
			StepIndex:           t.StepIndex,
			StateSnapshot:       result,
			CreatedAt:           time.Now(),
		}); err != nil {
			return fmt.Errorf("failed to save checkpoint at step %d: %w", t.StepIndex, err)
		}

		if err := i.historyRepo.Append(txCtx, &workflow.HistoryEvent{
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
			// All steps done — workflow is complete.
			if err := workflow.ValidateTransition(exec.State, workflow.StateCompleted); err != nil {
				return fmt.Errorf("cannot mark execution complete: %w", err)
			}
			if err := i.executionRepo.UpdateState(txCtx, exec.ID, workflow.StateCompleted); err != nil {
				return fmt.Errorf("failed to mark execution complete: %w", err)
			}
			if err := i.executionRepo.SaveResult(txCtx, exec.ID, result); err != nil {
				return fmt.Errorf("failed to save execution result: %w", err)
			}
			if err := i.historyRepo.Append(txCtx, &workflow.HistoryEvent{
				WorkflowExecutionID: exec.ID,
				EventType:           workflow.EventWorkflowCompleted,
				Payload:             result,
				CreatedAt:           time.Now(),
			}); err != nil {
				log.Printf("warn: failed to save history event: %v", err)
			}
			_ = i.taskRepo.Delete(txCtx, t.ID)
			// Drain signals on clean completion.
			i.signals.Drain(exec.ID)
			return nil
		}

		nextStep, err := i.workflowStepRepo.GetByDefinitionAndIndex(txCtx, exec.WorkflowDefinitionID, nextIndex)
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
			Input:               result,
			State:               workflow.StateCreated,
			Attempt:             0,
			MaxAttempts:         nextStep.MaxAttempts,
			ScheduledAt:         time.Now(),
			TimeoutSeconds:      nextStep.TimeoutSeconds, 
		}
		if err := i.taskRepo.Create(txCtx, nextTask); err != nil {
			return fmt.Errorf("failed to save next task: %w", err)
		}
		
		// Queue broker notification for after transaction commits
		notifyQueue = resolvedQueue

		if err := i.executionRepo.UpdateStepCursor(txCtx, exec.ID, nextIndex); err != nil {
			return fmt.Errorf("failed to advance step cursor: %w", err)
		}
		if err := i.historyRepo.Append(txCtx, &workflow.HistoryEvent{
			WorkflowExecutionID: exec.ID,
			StepIndex:           &nextIndex,
			StepName:            &nextStep.StepName,
			EventType:           workflow.EventStepScheduled,
			CreatedAt:           time.Now(),
		}); err != nil {
			log.Printf("warn: failed to save history event: %v", err)
		}

		_ = i.taskRepo.Delete(txCtx, t.ID)
		return nil
	})

	if err != nil {
		return err
	}

	// Trigger notification safely outside the transaction boundary
	if notifyQueue != "" {
		i.broker.Notify(notifyQueue)
	}

	return nil
}
