package usecase

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (w *workflowInteractor) RegisterWorkflow(ctx context.Context, name string, workflowType string, stepNames []string, compensationStepNames []string, defaultTimeoutSeconds int) (*workflow.WorkflowDefinition, error){

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

	existingDef, err := w.workflowDefRepo.GetByName(ctx, ns.ID, name)
	if err == nil && existingDef != nil {
		existingSteps, err := w.workflowStepRepo.GetStepsByDefinitionID(ctx, existingDef.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to load existing steps: %w", err)
		}

		if stepListsEqual(existingSteps, stepNames, compensationStepNames) {
			return existingDef, nil
		}

		if err := w.workflowDefRepo.Deactivate(ctx, existingDef.ID); err != nil {
			return nil, fmt.Errorf("failed to deactivate previous definition: %w", err)
		}
		nextVersion = existingDef.Version + 1
	} else if err != nil && !errors.Is(err, workflow.ErrNotFound) {
		return nil, fmt.Errorf("failed to check existing workflow definition: %w", err)
	}

wf := &workflow.WorkflowDefinition{
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

	if err := w.workflowDefRepo.Create(ctx, wf); err != nil {
		return nil, fmt.Errorf("failed to create workflow: %w", err)
	}

	for idx, stepName := range stepNames {
		var compName string
		if idx < len(compensationStepNames) {
			compName = compensationStepNames[idx]
		}
		step := &workflow.WorkflowDefinitionStep{
			ID:                   uuid.New().String(),
			WorkflowDefinitionID: wf.ID,
			StepName:             stepName,
			CompensationStepName: compName,
			StepIndex:            idx + 1,
		}
		if err := w.workflowStepRepo.Create(ctx, step); err != nil {
			return nil, fmt.Errorf("failed to create workflow steps: %w", err)
		}
	}
	return wf, nil
}


func stepListsEqual(existing []workflow.WorkflowDefinitionStep, requestedSteps []string, requestedCompensations []string) bool {
	if len(existing) != len(requestedSteps) {
		return false
	}
	for i, step := range existing {
		if step.StepName != requestedSteps[i] {
			return false
		}
		var reqComp string
		if i < len(requestedCompensations) {
			reqComp = requestedCompensations[i]
		}
		if step.CompensationStepName != reqComp {
			return false
		}
	}
	return true
}