package persistence

import (
	"context"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (s *PostgresWorkflowDefinitionRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE EXTENSION IF NOT EXISTS "pgcrypto";

		CREATE TABLE IF NOT EXISTS workflow_definition (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			namespace_id UUID NOT NULL,
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
			namespace_id,
			workflow_type,
			steps
		)
		VALUES ($1, $2, $3, $4)
		RETURNING
			id,
			created_at
		`,
		w.NamespaceID,
		w.WorkflowType,
		w.Steps,
	).Scan(
		&w.ID,
		&w.CreatedAt,
	)
}
func (s *PostgresWorkflowDefinitionRepository) GetByNamespaceID(
	ctx context.Context,
	namespaceID string,
) ([]workflow.WorkflowDefinition, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT id, namespace_id, workflow_type, created_at
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
			&def.WorkflowType,
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
