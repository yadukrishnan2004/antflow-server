package usecase

import (
    "context"
    "errors"
    "fmt"
    "time"

    "github.com/google/uuid"
    "github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (w *workflowInteractor) RegisterNameSpace(ctx context.Context, name string) (string, error) {
    existing, err := w.namespaceRepo.GetByName(ctx, name)
    if err != nil && !errors.Is(err, workflow.ErrNotFound) {
        return "", fmt.Errorf("failed to check namespace: %w", err)
    }
    if existing != nil {
        return "", fmt.Errorf("namespace %q already exists", name)
    }

    ns := &workflow.Namespace{
        ID:        uuid.New().String(),
        Name:      name,
        CreatedAt: time.Now(),
    }

    if err := w.namespaceRepo.Create(ctx, ns); err != nil {
        return "", fmt.Errorf("failed to create namespace: %w", err)
    }

    return ns.ID, nil
}

func (w *workflowInteractor) RegisterWorkflow(ctx context.Context, name string, workflowType string, stepNames []string) (*workflow.WorkflowDefinition, error) {
 
    ns, err := w.namespaceRepo.GetByName(ctx, name)
    if err != nil {
        if errors.Is(err, workflow.ErrNotFound) {
            return nil, fmt.Errorf("namespace %q not found", name)
        }
        return nil, fmt.Errorf("failed to get namespace: %w", err)
    }

    wf := &workflow.WorkflowDefinition{
        ID:            uuid.New().String(),
        NamespaceID:   ns.ID,
        WorkflowType:  workflowType,
		Steps: len(stepNames),
        CreatedAt:     time.Now(),
    }
	
	if err := w.workflowRepo.Create(ctx, wf); err != nil {
		return nil, fmt.Errorf("failed to create workflow: %w", err)
	}

	steps := make([]*workflow.WorkflowDefinitionStep, len(stepNames))
    for i, stepName := range stepNames {
        steps[i] = &workflow.WorkflowDefinitionStep{
            ID:           uuid.New().String(),
            WorkflowDefinitionID:   wf.ID,
            StepName:     stepName,
            StepIndex:    i + 1,
        }
		if err := w.workflowStepRepo.BatchCreate(ctx, steps[i]); err != nil {
			return nil, fmt.Errorf("failed to create workflow steps: %w", err)
		}
    }
    return wf, nil
}