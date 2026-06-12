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
	return i.taskRepo.FindAndLockPendingTask(taskQueue)
}

func (i *workflowInteractor) CompleteTask(ctx context.Context, taskID string, result []byte, errString string) error {
	if err := i.taskRepo.UpdateTaskComplete(taskID, result, errString); err != nil {
		return fmt.Errorf("failed to complete task: %w", err)
	}

	t, err := i.taskRepo.FindTaskByID(taskID)
	if err != nil {
		return fmt.Errorf("failed to find task: %w", err)
	}

	exec, err := i.workflowRepo.FindExecutionByID(t.WorkflowExecutionID)
	if err != nil {
		return fmt.Errorf("failed to find execution: %w", err)
	}

	if errString != "" {
		if t.Attempt < t.MaxAttempts {
			if err := i.historyRepo.SaveEvent(&workflow.HistoryEvent{
				WorkflowExecutionID: exec.ID,
				StepIndex:           &t.StepIndex,
				StepName:            &t.StepName,
				EventType:           "StepRetrying",
				Error:               errString,
				CreatedAt:           time.Now(),
			}); err != nil {
				log.Printf("warn: failed to save history event: %v", err)
			}
			return nil
		}
		if err := i.workflowRepo.UpdateExecutionState(exec.ID, workflow.StateFailed); err != nil {
			return fmt.Errorf("failed to mark execution failed: %w", err)
		}
		if err := i.historyRepo.SaveEvent(&workflow.HistoryEvent{
			WorkflowExecutionID: exec.ID,
			StepIndex:           &t.StepIndex,
			StepName:            &t.StepName,
			EventType:           "StepFailed",
			Error:               errString,
			CreatedAt:           time.Now(),
		}); err != nil {
			log.Printf("warn: failed to save history event: %v", err)
		}

		if err := i.historyRepo.SaveEvent(&workflow.HistoryEvent{
			WorkflowExecutionID: exec.ID,
			EventType:           "WorkflowExecutionFailed",
			Error:               errString,
			CreatedAt:           time.Now(),
		}); err != nil {
			log.Printf("warn: failed to save history event: %v", err)
		}
		return nil
	}

	if exec.WorkflowType == workflow.IndependentWorkflow {
		// 1. Write StepCompleted history (best-effort)
		if err := i.historyRepo.SaveEvent(&workflow.HistoryEvent{
			WorkflowExecutionID: exec.ID,
			StepIndex:           &t.StepIndex,
			StepName:            &t.StepName,
			EventType:           "StepCompleted",
			Payload:             result,
			CreatedAt:           time.Now(),
		}); err != nil {
			log.Printf("warn: failed to save history event: %v", err)
		}

		// 2. Check completed count
		completedCount, err := i.taskRepo.CountCompletedTasks(exec.ID)
		if err != nil {
			return fmt.Errorf("failed to count completed tasks: %w", err)
		}

		if completedCount == exec.TotalSteps {
			// All steps done! Collect outputs.
			outputs, err := i.taskRepo.GetAllTaskOutputs(exec.ID)
			if err != nil {
				return fmt.Errorf("failed to get task outputs: %w", err)
			}

			combinedBytes, err := json.Marshal(outputs)
			if err != nil {
				return fmt.Errorf("failed to marshal combined outputs: %w", err)
			}

			if err := i.workflowRepo.UpdateExecutionState(exec.ID, workflow.StateCompleted); err != nil {
				return fmt.Errorf("failed to mark execution complete: %w", err)
			}
			if err := i.workflowRepo.SaveResult(exec.ID, combinedBytes); err != nil {
				return fmt.Errorf("failed to save execution result: %w", err)
			}
			if err := i.historyRepo.SaveEvent(&workflow.HistoryEvent{
				WorkflowExecutionID: exec.ID,
				EventType:           "WorkflowExecutionCompleted",
				Payload:             combinedBytes,
				CreatedAt:           time.Now(),
			}); err != nil {
				log.Printf("warn: failed to save history event: %v", err)
			}
		}
		return nil
	}

	// CHAIN workflow behavior
	if err := i.checkpointRepo.SaveCheckpoint(&workflow.Checkpoint{
		WorkflowExecutionID: exec.ID,
		StepIndex:           t.StepIndex,
		StateSnapshot:       result,
		CreatedAt:           time.Now(),
	}); err != nil {
		return fmt.Errorf("failed to save checkpoint at step %d: %w", t.StepIndex, err)
	}

	// Write StepCompleted history (best-effort)
	if err := i.historyRepo.SaveEvent(&workflow.HistoryEvent{
		WorkflowExecutionID: exec.ID,
		StepIndex:           &t.StepIndex,
		StepName:            &t.StepName,
		EventType:           "StepCompleted",
		Payload:             result,
		CreatedAt:           time.Now(),
	}); err != nil {
		log.Printf("warn: failed to save history event: %v", err)
	}

	nextIndex := t.StepIndex + 1

	if nextIndex >= exec.TotalSteps {
		// All steps done
		if err := i.workflowRepo.UpdateExecutionState(exec.ID, workflow.StateCompleted); err != nil {
			return fmt.Errorf("failed to mark execution complete: %w", err)
		}
		if err := i.workflowRepo.SaveResult(exec.ID, result); err != nil {
			return fmt.Errorf("failed to save execution result: %w", err)
		}
		if err := i.historyRepo.SaveEvent(&workflow.HistoryEvent{
			WorkflowExecutionID: exec.ID,
			EventType:           "WorkflowExecutionCompleted",
			Payload:             result,
			CreatedAt:           time.Now(),
		}); err != nil {
			log.Printf("warn: failed to save history event: %v", err)
		}
		return nil
	}

	nextStep, err := i.workflowRepo.FindStep(exec.WorkflowName, nextIndex)
	if err != nil {
		return fmt.Errorf("failed to find next step definition: %w", err)
	}

	resolvedQueue := nextStep.TaskQueue
	if resolvedQueue == "" || resolvedQueue == "default" {
		resolvedQueue = exec.TaskQueue
	}

	nextTask := &workflow.Task{
		ID:                  fmt.Sprintf("task-%s", uuid.New().String()),
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
	if err := i.taskRepo.SaveTask(nextTask); err != nil {
		return fmt.Errorf("failed to save next task: %w", err)
	}
	i.taskBroker.Notify(resolvedQueue)

	if err := i.workflowRepo.UpdateStepCursor(exec.ID, nextIndex); err != nil {
		return fmt.Errorf("failed to advance step cursor: %w", err)
	}
	if err := i.historyRepo.SaveEvent(&workflow.HistoryEvent{
		WorkflowExecutionID: exec.ID,
		StepIndex:           &nextIndex,
		StepName:            &nextStep.StepName,
		EventType:           "StepScheduled",
		CreatedAt:           time.Now(),
	}); err != nil {
		log.Printf("warn: failed to save history event: %v", err)
	}

	return nil
}
