package workflow

type EventType = string

const (
	// Workflow-level lifecycle events.
	EventWorkflowStarted   EventType = "WORKFLOW_STARTED"
	EventWorkflowCompleted EventType = "WORKFLOW_COMPLETED"
	EventWorkflowFailed    EventType = "WORKFLOW_FAILED"
	EventWorkflowCancelled EventType = "WORKFLOW_CANCELLED"
	EventWorkflowTimedOut  EventType = "WORKFLOW_TIMED_OUT"
 
	// Step-level events within a workflow execution.
	EventStepScheduled EventType = "STEP_SCHEDULED"
	EventStepCompleted EventType = "STEP_COMPLETED"
	EventStepFailed    EventType = "STEP_FAILED"
	EventStepRetrying  EventType = "STEP_RETRYING"

	// Saga-level events
	EventCompensationStarted   EventType = "COMPENSATION_STARTED"
	EventCompensationCompleted EventType = "COMPENSATION_COMPLETED"
	EventCompensationFailed    EventType = "COMPENSATION_FAILED"
	EventSagaRolledBack        EventType = "SAGA_ROLLED_BACK"
	EventSagaRollbackFailed    EventType = "SAGA_ROLLBACK_FAILED"
)