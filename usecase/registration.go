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

	// Check if the workflow definition already exists and is active
	existingDef, err := w.workflowDefRepo.GetByName(ctx, ns.ID, name)
	if err == nil && existingDef != nil {
		return existingDef, nil
	}
	if err != nil && !errors.Is(err, workflow.ErrNotFound) {
		return nil, fmt.Errorf("failed to check existing workflow definition: %w", err)
	}

	wf := &workflow.WorkflowDefinition{
		ID:           uuid.New().String(),
		NamespaceID:  ns.ID,
		Name:         name,
		WorkflowType: workflowType,
		Steps:        len(stepNames),
		IsActive:     true,
		CreatedAt:    time.Now(),
	}

	if err := w.workflowDefRepo.Create(ctx, wf); err != nil {
		return nil, fmt.Errorf("failed to create workflow: %w", err)
	}

	steps := make([]*workflow.WorkflowDefinitionStep, len(stepNames))
	for i, stepName := range stepNames {
		steps[i] = &workflow.WorkflowDefinitionStep{
			ID:                   uuid.New().String(),
			WorkflowDefinitionID: wf.ID,
			StepName:             stepName,
			StepIndex:            i + 1,
		}
		if err := w.workflowStepRepo.BatchCreate(ctx, steps[i]); err != nil {
			return nil, fmt.Errorf("failed to create workflow steps: %w", err)
		}
	}
	return wf, nil
}
