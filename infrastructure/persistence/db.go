package persistence

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type Storage struct {
	Namespace              *PostgresNamespaceRepository
	WorkflowDefinition     *PostgresWorkflowDefinitionRepository
	WorkflowDefinitionStep *PostgresWorkflowDefinitionStepRepository
	WorkflowExecution      *PostgresWorkflowExecutionRepository
	Task                   *PostgresTaskRepository
	HistoryEvent           *PostgresHistoryEventRepository
	Checkpoint             *PostgresCheckpointRepository
}

type PostgresNamespaceRepository struct {
	db *sql.DB
}

type PostgresWorkflowDefinitionRepository struct {
	db *sql.DB
}

type PostgresWorkflowDefinitionStepRepository struct {
	db *sql.DB
}

type PostgresWorkflowExecutionRepository struct {
	db *sql.DB
}

type PostgresTaskRepository struct {
	db *sql.DB
}

type PostgresHistoryEventRepository struct {
	db *sql.DB
}

type PostgresCheckpointRepository struct {
	db *sql.DB
}

func New(dsn string) (*Storage, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("store: failed to ping database: %w", err)
	}

	storage := &Storage{
		Namespace:              &PostgresNamespaceRepository{db: db},
		WorkflowDefinition:     &PostgresWorkflowDefinitionRepository{db: db},
		WorkflowDefinitionStep: &PostgresWorkflowDefinitionStepRepository{db: db},
		WorkflowExecution:      &PostgresWorkflowExecutionRepository{db: db},
		Task:                   &PostgresTaskRepository{db: db},
		HistoryEvent:           &PostgresHistoryEventRepository{db: db},
		Checkpoint:             &PostgresCheckpointRepository{db: db},
	}

	return storage, nil
}
