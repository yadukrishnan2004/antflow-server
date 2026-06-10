package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

type WorkflowService interface {
	RegisterWorkflow(name string) (*workflow.WorkflowDefinition, error)
	StartWorkflow(workflowName string, taskQueue string, input []byte) (*workflow.Task, error)
	PollTask(ctx context.Context, taskQueue string) (*workflow.Task, error)
	CompleteTask(ctx context.Context, taskID string, result []byte, errString string) error
	GetWorkflowResult(ctx context.Context, workflowID string) (*workflow.WorkflowExecution, error)
	CancelWorkflow(ctx context.Context, workflowID string) error
	GetHistory(ctx context.Context, workflowID string) ([]workflow.HistoryEvent, error)
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
func (i *workflowInteractor) RegisterWorkflow(name string) (*workflow.WorkflowDefinition, error) {
	if name == "" {
        return nil, fmt.Errorf("workflow name cannot be empty")
    }
	def := &workflow.WorkflowDefinition{
		Name:      name,
		CreatedAt: time.Now(), 
	}

	existing, errs := i.workflowRepo.FindDefinitionByName(name)
    if errs != nil {
        return nil, fmt.Errorf("failed to check existing workflow: %w", errs)
    }

	if existing != nil {
            return nil, fmt.Errorf("%w: cannot re-register", name,)
    }


	err := i.workflowRepo.SaveDefinition(def)
	if err != nil {
		return nil, fmt.Errorf("failed to save workflow definition: %w", err)
	}

	return def, nil
}
//===============================================================================================================================================================================
func (i *workflowInteractor) StartWorkflow(workflowName string, taskQueue string, input []byte) (*workflow.Task, error) {
	// 1. Ensure definition exists
	def, err := i.workflowRepo.FindDefinitionByName(workflowName)
	if err != nil {
		return nil, fmt.Errorf("workflow definition not found: %w", err)
	}

	// 2. Create Execution
	exec := &workflow.WorkflowExecution{
		ID:               fmt.Sprintf("run-%s", uuid.New().String()),
		WorkflowName:     workflowName,
		TaskQueue:        taskQueue,
		State:            workflow.StateRunning,
		Input:            input,
		CurrentStepIndex: 0,
		TotalSteps:       len(def.Steps), // Assuming def is retrieved above
		CreatedAt:        time.Now(),
		UpdatedAt:        time.Now(),
	}

	err = i.workflowRepo.SaveExecution(exec)
	if err != nil {
		return nil, fmt.Errorf("failed to save workflow execution: %w", err)
	}

	var stepName string
	if len(def.Steps) > 0 {
		stepName = def.Steps[0].StepName
		if taskQueue == "default" && def.Steps[0].TaskQueue != "" {
			taskQueue = def.Steps[0].TaskQueue
		}
	} else {
		stepName = workflowName
	}

	t := &workflow.Task{
		ID:                  fmt.Sprintf("task-%s", uuid.New().String()),
		WorkflowExecutionID: exec.ID,
		TaskQueue:           taskQueue,
		StepIndex:           0,
		StepName:            stepName,
		Input:               input,
		State:               workflow.StateCreated,
		Attempt:             1,
		MaxAttempts:         3,
	}

	if err := i.taskRepo.SaveTask(t); err != nil {
		return nil, fmt.Errorf("failed to save task: %w", err)
	}

	return t, nil
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
			i.historyRepo.SaveEvent(&workflow.HistoryEvent{
                WorkflowExecutionID: exec.ID,
                StepIndex:           &t.StepIndex,
                StepName:            &t.StepName,
                EventType:           "StepRetrying",
                Error:               errString,
                CreatedAt:           time.Now(),
            })
			return nil
		}
		i.workflowRepo.UpdateExecutionState(exec.ID, workflow.StateFailed)
		i.historyRepo.SaveEvent(&workflow.HistoryEvent{
				WorkflowExecutionID: exec.ID,
				StepIndex:           &t.StepIndex,
				StepName:            &t.StepName,
				EventType:           "StepFailed",
				Error:               errString,
				CreatedAt:           time.Now(),
		})
	
		i.historyRepo.SaveEvent(&workflow.HistoryEvent{
				WorkflowExecutionID: exec.ID,
				EventType:           "WorkflowExecutionFailed",
				Error:               errString,
				CreatedAt:           time.Now(),
			})
			return nil
	}

	i.checkpointRepo.SaveCheckpoint(&workflow.Checkpoint{
        WorkflowExecutionID: exec.ID,
        StepIndex:           t.StepIndex,
        StateSnapshot:       result,
        CreatedAt:           time.Now(),
    })

    // 6. Write StepCompleted history
    i.historyRepo.SaveEvent(&workflow.HistoryEvent{
        WorkflowExecutionID: exec.ID,
        StepIndex:           &t.StepIndex,
        StepName:            &t.StepName,
        EventType:           "StepCompleted",
        Payload:             result,
        CreatedAt:           time.Now(),
    })

	 nextIndex := t.StepIndex + 1

    if nextIndex >= exec.TotalSteps {
        // All steps done
        i.workflowRepo.UpdateExecutionState(exec.ID, workflow.StateCompleted)
        i.workflowRepo.SaveResult(exec.ID, result)
        i.historyRepo.SaveEvent(&workflow.HistoryEvent{
            WorkflowExecutionID: exec.ID,
            EventType:           "WorkflowExecutionCompleted",
            Payload:             result,
            CreatedAt:           time.Now(),
        })
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
	i.historyRepo.SaveEvent(&workflow.HistoryEvent{
        WorkflowExecutionID: exec.ID,
        StepIndex:           &nextIndex,
        StepName:            &nextStep.StepName,
        EventType:           "StepScheduled",
        CreatedAt:           time.Now(),
    })

    return nil
}
//===============================================================================================================================================================================