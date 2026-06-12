package usecase

import (
	"errors"
	"fmt"
	"time"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (i *workflowInteractor) RegisterWorkflow(name string, workflowType string, stepNames []string) (*workflow.WorkflowDefinition, error) {
	if name == "" {
		return nil, fmt.Errorf("workflow name cannot be empty")
	}
	if len(stepNames) == 0 {
		return nil, fmt.Errorf("workflow '%s' must have at least one step", name)
	}

	existing, err := i.workflowRepo.FindDefinitionByName(name)
	if err != nil && !errors.Is(err, workflow.ErrNotFound) {
		return nil, fmt.Errorf("failed to check existing workflow: %w", err)
	}

	if existing != nil {
		// Check type matches
		if string(existing.WorkflowType) != workflowType {
			return nil, fmt.Errorf("%w: '%s' registered as %s, cannot re-register as %s",
				workflow.ErrWorkflowAlreadyExists, name, existing.WorkflowType, workflowType)
		}
		// Check step count matches
		if len(existing.Steps) != len(stepNames) {
			return nil, fmt.Errorf("%w: '%s' has %d steps, cannot re-register with %d",
				workflow.ErrWorkflowAlreadyExists, name, len(existing.Steps), len(stepNames))
		}
		// Check each step name matches in order
		for idx, step := range existing.Steps {
			if step.StepName != stepNames[idx] {
				return nil, fmt.Errorf("%w: '%s' step %d mismatch — existing='%s' new='%s'",
					workflow.ErrWorkflowAlreadyExists, name, idx, step.StepName, stepNames[idx])
			}
		}
		// Identical definition — idempotent success
		return existing, nil
	}

	// New workflow — build and save
	def := &workflow.WorkflowDefinition{
		Name:         name,
		WorkflowType: workflow.WorkflowType(workflowType),
		CreatedAt:    time.Now(),
	}
	for idx, stepName := range stepNames {
		def.Steps = append(def.Steps, workflow.WorkflowDefinitionStep{
			WorkflowName:   name,
			StepIndex:      idx,
			StepName:       stepName,
			TaskQueue:      "default",
			TimeoutSeconds: 300,
		})
	}

	if err := i.workflowRepo.SaveDefinition(def); err != nil {
		return nil, fmt.Errorf("failed to save workflow definition: %w", err)
	}

	return def, nil
}
