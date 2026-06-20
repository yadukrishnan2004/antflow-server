package usecase

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (w *workflowInteractor) RegisterWorkflow(ctx context.Context, name string, workflowType string, stepNames []string) (*workflow.WorkflowDefinition, error) {

	ns, err := w.namespaceRepo.GetByName(ctx, name)
	if err != nil {
		if errors.Is(err, workflow.ErrNotFound) {
			ns = &workflow.Namespace{
				ID:        uuid.New().String(),
				Name:      name,
				CreatedAt: time.Now(),
			}
			if err := w.namespaceRepo.Create(ctx, ns); err != nil {
				return nil, fmt.Errorf("failed to auto-create namespace: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to get namespace: %w", err)
		}
	}

	nextVersion := 1

	// Check if the workflow definition already exists and is active.
	existingDef, err := w.workflowDefRepo.GetByName(ctx, ns.ID, name)
	if err == nil && existingDef != nil {
		existingSteps, err := w.workflowStepRepo.GetStepsByDefinitionID(ctx, existingDef.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to load existing steps: %w", err)
		}

		if stepListsEqual(existingSteps, stepNames) {
			// Re-registration with an identical step list is idempotent —
			// return the existing active definition unchanged. No version
			// bump, no deactivation, no churn.
			return existingDef, nil
		}

		// Step list differs from the active definition: this is a real
		// re-registration. Deactivate the current active row so the new one
		// can become active under the partial unique index
		// (namespace_id, name) WHERE is_active = TRUE, and bump the version
		// so definition history stays intact for past executions that still
		// reference the old definition ID.
		if err := w.workflowDefRepo.Deactivate(ctx, existingDef.ID); err != nil {
			return nil, fmt.Errorf("failed to deactivate previous definition: %w", err)
		}
		nextVersion = existingDef.Version + 1
	} else if err != nil && !errors.Is(err, workflow.ErrNotFound) {
		return nil, fmt.Errorf("failed to check existing workflow definition: %w", err)
	}

	wf := &workflow.WorkflowDefinition{
		ID:           uuid.New().String(),
		NamespaceID:  ns.ID,
		Name:         name,
		WorkflowType: workflow.WorkflowType(workflowType),
		Version:      nextVersion,
		Steps:        len(stepNames),
		IsActive:     true,
		CreatedAt:    time.Now(),
	}

	if err := w.workflowDefRepo.Create(ctx, wf); err != nil {
		return nil, fmt.Errorf("failed to create workflow: %w", err)
	}

	for idx, stepName := range stepNames {
		step := &workflow.WorkflowDefinitionStep{
			ID:                   uuid.New().String(),
			WorkflowDefinitionID: wf.ID,
			StepName:             stepName,
			StepIndex:            idx + 1,
		}
		if err := w.workflowStepRepo.Create(ctx, step); err != nil {
			return nil, fmt.Errorf("failed to create workflow steps: %w", err)
		}
	}
	return wf, nil
}

// stepListsEqual reports whether the persisted step definitions (ordered by
// step_index) match the requested step name list exactly — same names, same
// order, same count. Used to make RegisterWorkflow idempotent for unchanged
// definitions while still detecting real changes that require a new version.
func stepListsEqual(existing []workflow.WorkflowDefinitionStep, requested []string) bool {
	if len(existing) != len(requested) {
		return false
	}
	for i, step := range existing {
		if step.StepName != requested[i] {
			return false
		}
	}
	return true
}