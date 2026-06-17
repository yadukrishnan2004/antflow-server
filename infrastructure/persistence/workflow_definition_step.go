package persistence

import (
	"context"
	"database/sql"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (s *PostgresWorkflowDefinitionStepRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE EXTENSION IF NOT EXISTS "pgcrypto";

		CREATE TABLE IF NOT EXISTS workflow_definition_step (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			workflow_definition_id UUID NOT NULL,
			step_index INTEGER NOT NULL,
			step_name TEXT NOT NULL,
			task_queue TEXT,
			timeout_seconds INTEGER NOT NULL DEFAULT 300,

			CONSTRAINT fk_workflow_definition_step_definition
				FOREIGN KEY (workflow_definition_id)
				REFERENCES workflow_definition(id)
				ON DELETE CASCADE,

			CONSTRAINT uq_workflow_definition_step_order
				UNIQUE (workflow_definition_id, step_index)
		);
	`)
	return err
}

func (s *PostgresWorkflowDefinitionStepRepository) BatchCreate(
	ctx context.Context,
	step *workflow.WorkflowDefinitionStep,
) error {
	return s.db.QueryRowContext(
		ctx,
		`
		INSERT INTO workflow_definition_step (
			workflow_definition_id,
			step_index,
			step_name,
			task_queue,
			timeout_seconds
		)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id
		`,
		step.WorkflowDefinitionID,
		step.StepIndex,
		step.StepName,
		step.TaskQueue,
		step.TimeoutSeconds,
	).Scan(&step.ID)
}

func (s *PostgresWorkflowDefinitionStepRepository) GetStepsByDefinitionID(
	ctx context.Context,
	definitionID string,
) ([]workflow.WorkflowDefinitionStep, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT id, workflow_definition_id, step_index, step_name, COALESCE(task_queue, ''), timeout_seconds
         FROM workflow_definition_step
         WHERE workflow_definition_id = $1
         ORDER BY step_index ASC`,
		definitionID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var steps []workflow.WorkflowDefinitionStep
	for rows.Next() {
		var step workflow.WorkflowDefinitionStep
		if err := rows.Scan(
			&step.ID,
			&step.WorkflowDefinitionID,
			&step.StepIndex,
			&step.StepName,
			&step.TaskQueue,
			&step.TimeoutSeconds,
		); err != nil {
			return nil, err
		}
		steps = append(steps, step)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return steps, nil
}

func (s *PostgresWorkflowDefinitionStepRepository) GetByDefinitionAndIndex(
	ctx context.Context,
	definitionID string,
	stepIndex int,
) (*workflow.WorkflowDefinitionStep, error) {
	step := &workflow.WorkflowDefinitionStep{}
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, workflow_definition_id, step_index, step_name, COALESCE(task_queue, ''), timeout_seconds
         FROM workflow_definition_step
         WHERE workflow_definition_id = $1 AND step_index = $2`,
		definitionID,
		stepIndex,
	).Scan(
		&step.ID,
		&step.WorkflowDefinitionID,
		&step.StepIndex,
		&step.StepName,
		&step.TaskQueue,
		&step.TimeoutSeconds,
	)
	if err == sql.ErrNoRows {
		return nil, workflow.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return step, nil
}
