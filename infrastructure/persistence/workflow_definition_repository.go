package persistence

import (
	"context"
	"database/sql"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (s *PostgresWorkflowDefinitionRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE EXTENSION IF NOT EXISTS "pgcrypto";

		CREATE TABLE IF NOT EXISTS workflow_definition (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			namespace_id UUID NOT NULL,
			name TEXT NOT NULL,
			workflow_type TEXT NOT NULL DEFAULT 'CHAIN',
			steps INTEGER NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

			CONSTRAINT fk_workflow_definition_namespace
				FOREIGN KEY (namespace_id)
				REFERENCES namespace(id)
				ON DELETE CASCADE,

			CONSTRAINT uq_workflow_definition_namespace_name
				UNIQUE (namespace_id, name)
		);
	`)
	return err
}

func (s *PostgresWorkflowDefinitionRepository) Create(
	ctx context.Context,
	w *workflow.WorkflowDefinition,
) error {
	return s.db.QueryRowContext(
		ctx,
		`
		INSERT INTO workflow_definition (
			id,
			namespace_id,
			name,
			workflow_type,
			steps,
			created_at
		)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING
			created_at
		`,
		w.ID,
		w.NamespaceID,
		w.Name,
		w.WorkflowType,
		w.Steps,
		w.CreatedAt,
	).Scan(
		&w.CreatedAt,
	)
}

func (s *PostgresWorkflowDefinitionRepository) GetByID(
	ctx context.Context,
	id string,
) (*workflow.WorkflowDefinition, error) {
	def := &workflow.WorkflowDefinition{}
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, namespace_id, name, workflow_type, steps, created_at
         FROM workflow_definition
         WHERE id = $1`,
		id,
	).Scan(
		&def.ID,
		&def.NamespaceID,
		&def.Name,
		&def.WorkflowType,
		&def.Steps,
		&def.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, workflow.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return def, nil
}

func (s *PostgresWorkflowDefinitionRepository) GetByName(
	ctx context.Context,
	namespaceID string,
	name string,
) (*workflow.WorkflowDefinition, error) {
	def := &workflow.WorkflowDefinition{}
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, namespace_id, name, workflow_type, steps, created_at
         FROM workflow_definition
         WHERE namespace_id = $1 AND name = $2`,
		namespaceID,
		name,
	).Scan(
		&def.ID,
		&def.NamespaceID,
		&def.Name,
		&def.WorkflowType,
		&def.Steps,
		&def.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, workflow.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return def, nil
}

func (s *PostgresWorkflowDefinitionRepository) GetByNamespaceID(
	ctx context.Context,
	namespaceID string,
) ([]workflow.WorkflowDefinition, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT id, namespace_id, name, workflow_type, steps, created_at
         FROM workflow_definition
         WHERE namespace_id = $1
         ORDER BY created_at ASC`,
		namespaceID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var defs []workflow.WorkflowDefinition
	for rows.Next() {
		var def workflow.WorkflowDefinition
		if err := rows.Scan(
			&def.ID,
			&def.NamespaceID,
			&def.Name,
			&def.WorkflowType,
			&def.Steps,
			&def.CreatedAt,
		); err != nil {
			return nil, err
		}
		defs = append(defs, def)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return defs, nil
}
