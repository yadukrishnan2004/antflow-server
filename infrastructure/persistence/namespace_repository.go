package persistence

import (
	"context"
	"database/sql"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (s *PostgresNamespaceRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE EXTENSION IF NOT EXISTS "pgcrypto";

		CREATE TABLE IF NOT EXISTS namespace (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			name TEXT NOT NULL UNIQUE,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
	`)
	return err
}

func (s *PostgresNamespaceRepository) Create(
	ctx context.Context,
	ns *workflow.Namespace,
) error {
	_, err := s.db.ExecContext(
		ctx,
		`
		INSERT INTO namespace (
			id,
			name,
			created_at
		)
		VALUES ($1, $2, $3)
		`,
		ns.ID,
		ns.Name,
		ns.CreatedAt,
	)

	return err
}

func (s *PostgresNamespaceRepository) GetByID(
	ctx context.Context,
	id string,
) (*workflow.Namespace, error) {
	ns := &workflow.Namespace{}
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, name, created_at FROM namespace WHERE id = $1`,
		id,
	).Scan(&ns.ID, &ns.Name, &ns.CreatedAt)

	if err == sql.ErrNoRows {
		return nil, workflow.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	return ns, nil
}

func (s *PostgresNamespaceRepository) GetByName(
	ctx context.Context,
	name string,
) (*workflow.Namespace, error) {
	ns := &workflow.Namespace{}
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, name, created_at FROM namespace WHERE name = $1`,
		name,
	).Scan(&ns.ID, &ns.Name, &ns.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, workflow.ErrNotFound
		}
		return nil, err
	}
	return ns, nil
}

var _ workflow.NamespaceRepository = (*PostgresNamespaceRepository)(nil)
