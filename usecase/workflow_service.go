package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

type WorkflowService interface {
	RegisterWorkflow(name string, workflowType string, stepNames []string) (*workflow.WorkflowDefinition, error)
	StartWorkflow(workflowName string, taskQueue string, input []byte) (*workflow.WorkflowExecution, error)
	PollTask(ctx context.Context, taskQueue string) (*workflow.Task, error)
	CompleteTask(ctx context.Context, taskID string, result []byte, errString string) error
	GetWorkflowResult(ctx context.Context, workflowID string) (*workflow.WorkflowExecution, error)
	CancelWorkflow(ctx context.Context, workflowID string) error
	GetHistory(ctx context.Context, workflowID string) ([]workflow.HistoryEvent, error)
	GetWorkflowNameForExecution(executionID string) (string, error)
}

type workflowInteractor struct {
    workflowRepo   workflow.WorkflowRepository
    taskRepo       workflow.TaskRepository
    checkpointRepo workflow.CheckpointRepository  
    historyRepo    workflow.HistoryRepository
}

func NewWorkflowService(
    wRepo workflow.WorkflowRepository,
    tRepo workflow.TaskRepository,
    cRepo workflow.CheckpointRepository,  
    hRepo workflow.HistoryRepository,
) WorkflowService {
    return &workflowInteractor{
        workflowRepo:   wRepo,
        taskRepo:       tRepo,
        checkpointRepo: cRepo,
        historyRepo:    hRepo,
    }
}
//===============================================================================================================================================================================
func (i *workflowInteractor) RegisterWorkflow(name string, workflowType string, stepNames []string) (*workflow.WorkflowDefinition, error) {
	if name == "" {
		return nil, fmt.Errorf("workflow name cannot be empty")
	}
	if len(stepNames) == 0 {
		return nil, fmt.Errorf("workflow '%s' must have at least one step", name)
	}

	existing, err := i.workflowRepo.FindDefinitionByName(name)
	if err != nil && !errors.Is(err, workflow.ErrNotFound) {
		return nil, fmt.Errorf("failed to check existing workflow: %w", err)
	}

	if existing != nil {
		// Check type matches
		if string(existing.WorkflowType) != workflowType {
			return nil, fmt.Errorf("%w: '%s' registered as %s, cannot re-register as %s",
				workflow.ErrWorkflowAlreadyExists, name, existing.WorkflowType, workflowType)
		}
		// Check step count matches
		if len(existing.Steps) != len(stepNames) {
			return nil, fmt.Errorf("%w: '%s' has %d steps, cannot re-register with %d",
				workflow.ErrWorkflowAlreadyExists, name, len(existing.Steps), len(stepNames))
		}
		// Check each step name matches in order
		for idx, step := range existing.Steps {
			if step.StepName != stepNames[idx] {
				return nil, fmt.Errorf("%w: '%s' step %d mismatch — existing='%s' new='%s'",
					workflow.ErrWorkflowAlreadyExists, name, idx, step.StepName, stepNames[idx])
			}
		}
		// Identical definition — idempotent success
		return existing, nil
	}

	// New workflow — build and save
	def := &workflow.WorkflowDefinition{
		Name:         name,
		WorkflowType: workflow.WorkflowType(workflowType),
		CreatedAt:    time.Now(),
	}
	for idx, stepName := range stepNames {
		def.Steps = append(def.Steps, workflow.WorkflowDefinitionStep{
			WorkflowName:   name,
			StepIndex:      idx,
			StepName:       stepName,
			TaskQueue:      "default",
			TimeoutSeconds: 300,
		})
	}

	if err := i.workflowRepo.SaveDefinition(def); err != nil {
		return nil, fmt.Errorf("failed to save workflow definition: %w", err)
	}

	return def, nil
}
//===============================================================================================================================================================================
func (i *workflowInteractor) StartWorkflow(workflowName string, taskQueue string, input []byte) (*workflow.WorkflowExecution, error) {
    def, err := i.workflowRepo.FindDefinitionByName(workflowName)
    if err != nil {
        return nil, fmt.Errorf("workflow definition not found: %w", err)
    }
    if len(def.Steps) == 0 {
        return nil, fmt.Errorf("workflow '%s' has no steps registered", workflowName)
    }

    exec := &workflow.WorkflowExecution{
        ID:               fmt.Sprintf("run-%s", uuid.New().String()),
        WorkflowName:     workflowName,
        WorkflowType:     def.WorkflowType,
        TaskQueue:        taskQueue,
        State:            workflow.StateRunning,
        Input:            input,
        CurrentStepIndex: 0,
        TotalSteps:       len(def.Steps),
        CreatedAt:        time.Now(),
        UpdatedAt:        time.Now(),
    }

    if err := i.workflowRepo.SaveExecution(exec); err != nil {
        return nil, fmt.Errorf("failed to save workflow execution: %w", err)
    }

    i.historyRepo.SaveEvent(&workflow.HistoryEvent{
        WorkflowExecutionID: exec.ID,
        EventType:           "WorkflowStarted",
        Payload:             input,
        CreatedAt:           time.Now(),
    })

    switch def.WorkflowType {
    case workflow.IndependentWorkflow:
        for idx, step := range def.Steps {
            q := step.TaskQueue
            if q == "" {
                q = taskQueue
            }
            t := &workflow.Task{
                ID:                  fmt.Sprintf("task-%s", uuid.New().String()),
                WorkflowExecutionID: exec.ID,
                TaskQueue:           q,
                StepIndex:           idx,
                StepName:            step.StepName,
                Input:               input,
                State:               workflow.StateCreated,
                Attempt:             1,
                MaxAttempts:         3,
                ScheduledAt:         time.Now(),
            }
            if err := i.taskRepo.SaveTask(t); err != nil {
                return nil, fmt.Errorf("failed to save task for step %d: %w", idx, err)
            }
            stepIdx, stepName := idx, step.StepName
            i.historyRepo.SaveEvent(&workflow.HistoryEvent{
                WorkflowExecutionID: exec.ID,
                StepIndex:           &stepIdx,
                StepName:            &stepName,
                EventType:           "StepScheduled",
                CreatedAt:           time.Now(),
            })
        }

    default: // ChainWorkflow
        step0 := def.Steps[0]
        q := step0.TaskQueue
        if q == "" {
            q = taskQueue
        }
        t := &workflow.Task{
            ID:                  fmt.Sprintf("task-%s", uuid.New().String()),
            WorkflowExecutionID: exec.ID,
            TaskQueue:           q,
            StepIndex:           0,
            StepName:            step0.StepName,
            Input:               input,
            State:               workflow.StateCreated,
            Attempt:             1,
            MaxAttempts:         3,
            ScheduledAt:         time.Now(),
        }
        if err := i.taskRepo.SaveTask(t); err != nil {
            return nil, fmt.Errorf("failed to save step 0 task: %w", err)
        }
        stepIdx, stepName := 0, step0.StepName
        i.historyRepo.SaveEvent(&workflow.HistoryEvent{
            WorkflowExecutionID: exec.ID,
            StepIndex:           &stepIdx,
            StepName:            &stepName,
            EventType:           "StepScheduled",
            CreatedAt:           time.Now(),
        })
    }

    return exec, nil
}

//===============================================================================================================================================================================

func (i *workflowInteractor) PollTask(ctx context.Context, taskQueue string) (*workflow.Task, error) {
	return i.taskRepo.FindAndLockPendingTask(taskQueue)
}

//===============================================================================================================================================================================

func (i *workflowInteractor) GetWorkflowResult(ctx context.Context, workflowID string) (*workflow.WorkflowExecution, error) {
    return i.workflowRepo.FindExecutionByID(workflowID)
}

//===============================================================================================================================================================================

func (i *workflowInteractor) CancelWorkflow(ctx context.Context, workflowID string) error {
	exec, err := i.workflowRepo.FindExecutionByID(workflowID)
	if err != nil {
		return fmt.Errorf("failed to find execution: %w", err)
	}

	if exec.State == workflow.StateCompleted || exec.State == workflow.StateFailed || exec.State == workflow.StateCancelled {
		return fmt.Errorf("cannot cancel workflow in terminal state: %s", exec.State)
	}

	return i.workflowRepo.UpdateExecutionState(workflowID, workflow.StateCancelled)
}
//===============================================================================================================================================================================
func (i *workflowInteractor) GetHistory(ctx context.Context, workflowID string) ([]workflow.HistoryEvent, error) {
	return i.historyRepo.GetHistory(workflowID)
}
//===============================================================================================================================================================================
func (i *workflowInteractor) CompleteTask(ctx context.Context, taskID string, result []byte, errString string) error {
	if err := i.taskRepo.UpdateTaskComplete(taskID, result, errString);err != nil {
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
    if resolvedQueue == "" {
        resolvedQueue = exec.TaskQueue
    }

	nextTask := &workflow.Task{
        ID:                  fmt.Sprintf("task-%s", uuid.New().String()),
        WorkflowExecutionID: exec.ID,
        StepIndex:           nextIndex,
        StepName:            nextStep.StepName,
        TaskQueue:           resolvedQueue,
        Input:               result,          // ← chain
        State:               workflow.StateCreated,
        Attempt:             1,
        MaxAttempts:         3,
        ScheduledAt:         time.Now(),
    }
	if err := i.taskRepo.SaveTask(nextTask); err != nil {
        return fmt.Errorf("failed to save next task: %w", err)
    }

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

func (i *workflowInteractor) GetWorkflowNameForExecution(executionID string) (string, error) {
	return i.workflowRepo.GetWorkflowNameByExecutionID(executionID)
}

//===============================================================================================================================================================================