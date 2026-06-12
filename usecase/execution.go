package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

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
		notifiedQueues := make(map[string]bool)
		for idx, step := range def.Steps {
			q := step.TaskQueue
			if q == "" || q == "default" {
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
			notifiedQueues[q] = true
		}
		for q := range notifiedQueues {
			i.taskBroker.Notify(q)
		}

	default: // ChainWorkflow
		step0 := def.Steps[0]
		q := step0.TaskQueue
		if q == "" || q == "default" {
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
		i.taskBroker.Notify(q)
	}

	return exec, nil
}

func (i *workflowInteractor) GetWorkflowResult(ctx context.Context, workflowID string) (*workflow.WorkflowExecution, error) {
	return i.workflowRepo.FindExecutionByID(workflowID)
}

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

func (i *workflowInteractor) GetWorkflowNameForExecution(executionID string) (string, error) {
	return i.workflowRepo.GetWorkflowNameByExecutionID(executionID)
}
