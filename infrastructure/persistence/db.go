package persistence

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

// PostgresWorkflowRepository implements workflow.WorkflowRepository.
type PostgresWorkflowRepository struct {
	db *sql.DB
}

// PostgresTaskRepository implements workflow.TaskRepository.
type PostgresTaskRepository struct {
	db *sql.DB
}

// PostgresCheckpointRepository implements workflow.CheckpointRepository.
type PostgresCheckpointRepository struct {
	db *sql.DB
}

// PostgresHistoryRepository implements workflow.HistoryRepository.
type PostgresHistoryRepository struct {
	db *sql.DB
}

// New opens a Postgres connection, runs migrations for all repositories,
// and returns them together.
func New(dsn string) (*PostgresWorkflowRepository, *PostgresTaskRepository, *PostgresCheckpointRepository, *PostgresHistoryRepository, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("store: failed to open database: %w", err)
	}

	wRepo := &PostgresWorkflowRepository{db: db}
	tRepo := &PostgresTaskRepository{db: db}
	cRepo := &PostgresCheckpointRepository{db: db}
	hRepo := &PostgresHistoryRepository{db: db}

	if err := wRepo.Migrate(); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("store: workflow migration failed: %w", err)
	}

	if err := tRepo.Migrate(); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("store: task migration failed: %w", err)
	}

	if err := cRepo.Migrate(); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("store: checkpoint migration failed: %w", err)
	}

	if err := hRepo.Migrate(); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("store: history migration failed: %w", err)
	}

	return wRepo, tRepo, cRepo, hRepo, nil
}

// ---------------------------------------------------------------------------
// helpers for nullable columns
// ---------------------------------------------------------------------------

func nullableString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

func nullableBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}

func nullableTime(t time.Time) sql.NullTime {
	return sql.NullTime{Time: t, Valid: !t.IsZero()}
}
