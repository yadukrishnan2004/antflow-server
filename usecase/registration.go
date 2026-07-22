package usecase

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (w *workflowInteractor) RegisterWorkflow(ctx context.Context, name string, workflowType string, stepNames []string, compensationStepNames []string, defaultTimeoutSeconds int, stepMaxAttempts []int) (*workflow.WorkflowDefinition, error){

	// Business validation: Ensure only supported workflow types are processed
	wType := workflow.WorkflowType(workflowType)
	if wType != workflow.ChainWorkflow && 
	   wType != workflow.IndependentWorkflow && 
	   wType != workflow.SagaWorkflow {
		return nil, fmt.Errorf("invalid workflow type %q: must be CHAIN, INDEPENDENT, or SAGA", workflowType)
	}

	if len(stepNames) == 0 {
		return nil, fmt.Errorf("invalid workflow definition: must contain at least one step")
	}

	var wf *workflow.WorkflowDefinition

	err := w.txManager.RunInTx(ctx, func(txCtx context.Context) error {
		ns, err := w.namespaceRepo.GetByName(txCtx, name)
		if err != nil {
			if errors.Is(err, workflow.ErrNotFound) {
				ns = &workflow.Namespace{
					ID:        uuid.New().String(),
					Name:      name,
					CreatedAt: time.Now(),
				}
				if err := w.namespaceRepo.Create(txCtx, ns); err != nil {
					return fmt.Errorf("failed to auto-create namespace: %w", err)
				}
			} else {
				return fmt.Errorf("failed to get namespace: %w", err)
			}
		}

		nextVersion := 1

		// 1. Retrieve the currently active version of this workflow
		existingDef, err := w.workflowDefRepo.GetByName(txCtx, ns.ID, name)
		if err == nil && existingDef != nil {
			// Always deactivate the previous version
			if err := w.workflowDefRepo.Deactivate(txCtx, existingDef.ID); err != nil {
				return fmt.Errorf("failed to deactivate previous definition: %w", err)
			}
			nextVersion = existingDef.Version + 1
		} else if err != nil && !errors.Is(err, workflow.ErrNotFound) {
			return fmt.Errorf("failed to check existing workflow definition: %w", err)
		}

		// 2. Prepare the new workflow version definition (no MaxAttempts here)
		wf = &workflow.WorkflowDefinition{
			ID:                    uuid.New().String(),
			NamespaceID:           ns.ID,
			Name:                  name,
			WorkflowType:          workflow.WorkflowType(workflowType),
			Version:               nextVersion,
			Steps:                 len(stepNames),
			DefaultTimeoutSeconds: defaultTimeoutSeconds,
			IsActive:              true,
			CreatedAt:             time.Now(),
		}

		if err := w.workflowDefRepo.Create(txCtx, wf); err != nil {
			return fmt.Errorf("failed to create workflow: %w", err)
		}

		// 3. Create the definition steps (Assign MaxAttempts to each step here)
		for idx, stepName := range stepNames {
			var compName string
			if idx < len(compensationStepNames) {
				compName = compensationStepNames[idx]
			}
			
			maxAttempts := 3 // default fallback
			if idx < len(stepMaxAttempts) && stepMaxAttempts[idx] > 0 {
				maxAttempts = stepMaxAttempts[idx]
			}

			timeout := defaultTimeoutSeconds
			if timeout <= 0 {
				timeout = 300
			}

			step := &workflow.WorkflowDefinitionStep{
				ID:                   uuid.New().String(),
				WorkflowDefinitionID: wf.ID,
				StepName:             stepName,
				CompensationStepName: compName,
				StepIndex:            idx + 1,
				MaxAttempts:          maxAttempts,
				TimeoutSeconds:       timeout,
			}
			if err := w.workflowStepRepo.Create(txCtx, step); err != nil {
				return fmt.Errorf("failed to create workflow steps: %w", err)
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return wf, nil
}