package persistence

import (
	"context"
	"database/sql"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

// Migrate creates the workflow_definition table and its indexes.
//
// Key schema decisions vs the old version:
//
//   - The plain UNIQUE (namespace_id, name) constraint is GONE. It prevented
//     re-registering a workflow after its previous version was deactivated.
//   - Instead we have a PARTIAL unique index on (namespace_id, name) WHERE
//     is_active = TRUE, which is the only uniqueness we actually need: at most
//     one active definition per (namespace, name) pair.
//   - The version uniqueness constraint stays, so history rows are stable.
func (s *PostgresWorkflowDefinitionRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE EXTENSION IF NOT EXISTS "pgcrypto";

		CREATE TABLE IF NOT EXISTS workflow_definition (
			id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
			namespace_id  UUID        NOT NULL,
			name          TEXT        NOT NULL,
			workflow_type TEXT        NOT NULL DEFAULT 'CHAIN',
			version       INT         NOT NULL DEFAULT 1,
			is_active     BOOLEAN     NOT NULL DEFAULT TRUE,
			steps         INT         NOT NULL DEFAULT 0,
			created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

			CONSTRAINT fk_workflow_definition_namespace
				FOREIGN KEY (namespace_id)
				REFERENCES namespace(id)
				ON DELETE CASCADE,

			CONSTRAINT uq_workflow_definition_namespace_name_version
				UNIQUE (namespace_id, name, version)
		);
		-- Only one active definition per (namespace, name) at a time.
		-- A partial index is the correct tool here; a plain UNIQUE column
		-- constraint cannot express the WHERE clause.
		CREATE UNIQUE INDEX IF NOT EXISTS uq_workflow_definition_active
			ON workflow_definition (namespace_id, name)
			WHERE is_active = TRUE;
		
	ALTER TABLE workflow_definition
    ADD COLUMN IF NOT EXISTS default_timeout_seconds INTEGER NOT NULL DEFAULT 0;

	`)
	return err
}

func (s *PostgresWorkflowDefinitionRepository) Create(
	ctx context.Context, w *workflow.WorkflowDefinition,
) error {
	if w.Version == 0 {
		w.Version = 1
	}
	return s.db.QueryRowContext(ctx, `
		INSERT INTO workflow_definition
			(id, namespace_id, name, workflow_type, version, is_active, steps, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING created_at
	`,
		w.ID, w.NamespaceID, w.Name, string(w.WorkflowType),
		w.Version, w.IsActive, w.Steps, w.CreatedAt,
	).Scan(&w.CreatedAt)
}

// Deactivate marks the definition with the given id as inactive.
// Returns ErrNotFound if no row with that id exists.
func (s *PostgresWorkflowDefinitionRepository) Deactivate(
	ctx context.Context, id string,
) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE workflow_definition
		SET    is_active = FALSE
		WHERE  id = $1
	`, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return workflow.ErrNotFound
	}
	return nil
}

func (s *PostgresWorkflowDefinitionRepository) GetByID(
	ctx context.Context, id string,
) (*workflow.WorkflowDefinition, error) {
	def := &workflow.WorkflowDefinition{}
	var wfType string
	err := s.db.QueryRowContext(ctx, `
		SELECT id, namespace_id, name, workflow_type, version, is_active, steps, created_at
		FROM   workflow_definition
		WHERE  id = $1
	`, id).Scan(
		&def.ID, &def.NamespaceID, &def.Name, &wfType,
		&def.Version, &def.IsActive, &def.Steps, &def.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, workflow.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	def.WorkflowType = workflow.WorkflowType(wfType)
	return def, nil
}

func (s *PostgresWorkflowDefinitionRepository) GetByName(
	ctx context.Context, namespaceID, name string,
) (*workflow.WorkflowDefinition, error) {
	def := &workflow.WorkflowDefinition{}
	var wfType string
	err := s.db.QueryRowContext(ctx, `
		SELECT id, namespace_id, name, workflow_type, version, is_active, steps, created_at
		FROM   workflow_definition
		WHERE  namespace_id = $1 AND name = $2 AND is_active = TRUE
	`, namespaceID, name).Scan(
		&def.ID, &def.NamespaceID, &def.Name, &wfType,
		&def.Version, &def.IsActive, &def.Steps, &def.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, workflow.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	def.WorkflowType = workflow.WorkflowType(wfType)
	return def, nil
}

func (s *PostgresWorkflowDefinitionRepository) GetByNamespaceID(
	ctx context.Context, namespaceID string,
) ([]workflow.WorkflowDefinition, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, namespace_id, name, workflow_type, version, is_active, steps, created_at
		FROM   workflow_definition
		WHERE  namespace_id = $1
		ORDER  BY created_at ASC
	`, namespaceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var defs []workflow.WorkflowDefinition
	for rows.Next() {
		var def workflow.WorkflowDefinition
		var wfType string
		if err := rows.Scan(
			&def.ID, &def.NamespaceID, &def.Name, &wfType,
			&def.Version, &def.IsActive, &def.Steps, &def.CreatedAt,
		); err != nil {
			return nil, err
		}
		def.WorkflowType = workflow.WorkflowType(wfType)
		defs = append(defs, def)
	}
	return defs, rows.Err()
}

var _ workflow.WorkflowDefinitionRepository = (*PostgresWorkflowDefinitionRepository)(nil)