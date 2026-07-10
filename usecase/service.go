package usecase

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

type WorkflowService interface {
	RegisterWorkflow(ctx context.Context, name string, workflowType string, stepNames []string, compensationStepNames []string, defaultTimeoutSeconds int) (*workflow.WorkflowDefinition, error)
	StartWorkflow(ctx context.Context, workflowName string, taskQueue string, input []byte, timeoutOverrideSeconds int) (*workflow.WorkflowExecution, error)
	PollTask(ctx context.Context, taskQueue string) (*workflow.Task, error)
	CompleteTask(ctx context.Context, taskID string, result []byte, errString string) error
	PollCompensationTask(ctx context.Context, taskQueue string) (*workflow.CompensationTask, error)
	CompleteCompensationTask(ctx context.Context, taskID string, result []byte, errString string) error
	GetWorkflowResult(ctx context.Context, workflowID string) (*workflow.WorkflowExecution, error)
	CancelWorkflow(ctx context.Context, workflowID string) error
	GetHistory(ctx context.Context, workflowID string) ([]workflow.HistoryEvent, error)
	GetHistoryAfter(ctx context.Context, workflowID string, afterID int64) ([]workflow.HistoryEvent, error)
	GetWorkflowNameForExecution(ctx context.Context, executionID string) (string, error)
	SubscribeToQueue(taskQueue string) (<-chan struct{}, func())

	// Signal / pause-resume support.
	// SendSignal delivers payload to the step currently waiting for the named
	// signal on executionID, or buffers it for the next WaitForSignal call.
	SendSignal(executionID, name string, payload []byte) bool

	// WaitForSignal blocks until the named signal arrives for executionID or
	// timeout elapses. Zero timeout means wait indefinitely (bounded by ctx).
	WaitForSignal(ctx context.Context, executionID, name string, timeout time.Duration) ([]byte, error)
	ExpireOverdueWorkflows(ctx context.Context) error
}

type workflowInteractor struct {
	namespaceRepo        workflow.NamespaceRepository
	workflowDefRepo      workflow.WorkflowDefinitionRepository
	workflowStepRepo     workflow.WorkflowDefinitionStepRepository
	executionRepo        workflow.WorkflowExecutionRepository
	taskRepo             workflow.TaskRepository
	compensationTaskRepo workflow.CompensationTaskRepository
	historyRepo          workflow.HistoryEventRepository
	checkpointRepo       workflow.CheckpointRepository

	// broker is used for both regular and compensation task notifications.
	// Previously a separate compBroker field was declared but never initialized,
	// meaning compensation task notifies silently no-oped. Unifying on one
	// broker is correct: StreamTasks and StreamCompensationTasks both subscribe
	// to the same queue name and independently poll their respective repos;
	// being woken by the same notification is fine and keeps the code simpler.
	broker *TaskBroker

	// signals handles pause/resume for workflow steps.
	signals *SignalStore
}

func New(
	namespaceRepo workflow.NamespaceRepository,
	workflowDefRepo workflow.WorkflowDefinitionRepository,
	workflowStepRepo workflow.WorkflowDefinitionStepRepository,
	executionRepo workflow.WorkflowExecutionRepository,
	taskRepo workflow.TaskRepository,
	compensationTaskRepo workflow.CompensationTaskRepository,
	historyRepo workflow.HistoryEventRepository,
	checkpointRepo workflow.CheckpointRepository,
) WorkflowService {
	return &workflowInteractor{
		namespaceRepo:        namespaceRepo,
		workflowDefRepo:      workflowDefRepo,
		workflowStepRepo:     workflowStepRepo,
		executionRepo:        executionRepo,
		taskRepo:             taskRepo,
		compensationTaskRepo: compensationTaskRepo,
		historyRepo:          historyRepo,
		checkpointRepo:       checkpointRepo,
		broker:               NewTaskBroker(),
		signals:              NewSignalStore(),
	}
}

func (i *workflowInteractor) SubscribeToQueue(taskQueue string) (<-chan struct{}, func()) {
	return i.broker.Subscribe(taskQueue)
}

func (i *workflowInteractor) SendSignal(executionID, name string, payload []byte) bool {
	return i.signals.Send(executionID, name, payload)
}

func (i *workflowInteractor) WaitForSignal(ctx context.Context, executionID, name string, timeout time.Duration) ([]byte, error) {
	return i.signals.Wait(ctx, executionID, name, timeout)
}	

func (i *workflowInteractor) ExpireOverdueWorkflows(ctx context.Context) error {
    ids, err := i.executionRepo.ExpireOverdue(ctx)
    if err != nil {
        return fmt.Errorf("failed to expire overdue workflows: %w", err)
    }

    for _, id := range ids {
        if err := i.taskRepo.CancelByExecution(ctx, id); err != nil {
            log.Printf("warn: failed to cancel tasks for timed-out execution %s: %v", id, err)
        }
        if err := i.compensationTaskRepo.CancelByExecution(ctx, id); err != nil {
            log.Printf("warn: failed to cancel compensation tasks for timed-out execution %s: %v", id, err)
        }
        if err := i.historyRepo.Append(ctx, &workflow.HistoryEvent{
            WorkflowExecutionID: id,
            EventType:           workflow.EventWorkflowTimedOut,
            Error:               "workflow exceeded its configured timeout",
            CreatedAt:           time.Now(),
        }); err != nil {
            log.Printf("warn: failed to append timeout history event for %s: %v", id, err)
        }
        // Also record the standard WORKFLOW_FAILED event, so anything
        // consuming history and only checking for EventWorkflowFailed
        // (e.g. StreamWorkflowHistory's terminal-event detection) still
        // recognizes the stream should close.
        if err := i.historyRepo.Append(ctx, &workflow.HistoryEvent{
            WorkflowExecutionID: id,
            EventType:           workflow.EventWorkflowFailed,
            Error:               "workflow exceeded its configured timeout",
            CreatedAt:           time.Now(),
        }); err != nil {
            log.Printf("warn: failed to append workflow-failed event for %s: %v", id, err)
        }

        i.signals.Drain(id)

        log.Printf("info: workflow execution %s marked FAILED — exceeded configured timeout", id)
    }

    return nil
}