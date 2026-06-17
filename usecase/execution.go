package usecase

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (i *workflowInteractor) StartWorkflow(ctx context.Context, workflowName string, typ string, input []byte) (*workflow.WorkflowExecution, error) {
	ns , err := i.namespaceRepo.GetByName(ctx,workflowName)
	if err != nil {
		return nil , workflow.ErrNotFound
	}
	i.workflowRepo.GetByNamespaceID(ctx,ns.ID)



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
