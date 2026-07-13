package persistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

// Storage holds all repository implementations backed by a single *sql.DB.
type Storage struct {
	Namespace              *PostgresNamespaceRepository
	WorkflowDefinition     *PostgresWorkflowDefinitionRepository
	WorkflowDefinitionStep *PostgresWorkflowDefinitionStepRepository
	WorkflowExecution      *PostgresWorkflowExecutionRepository
	Task                   *PostgresTaskRepository
	HistoryEvent           *PostgresHistoryEventRepository
	Checkpoint             *PostgresCheckpointRepository
	CompensationTask       *PostgresCompensationTaskRepository
	TxManager              *PostgresTransactionManager
}

type PostgresNamespaceRepository struct{ db *sql.DB }
type PostgresWorkflowDefinitionRepository struct{ db *sql.DB }
type PostgresWorkflowDefinitionStepRepository struct{ db *sql.DB }
type PostgresWorkflowExecutionRepository struct{ db *sql.DB }
type PostgresTaskRepository struct{ db *sql.DB }
type PostgresHistoryEventRepository struct{ db *sql.DB }
type PostgresCheckpointRepository struct{ db *sql.DB }
type PostgresTransactionManager struct {db *sql.DB}

// DBConfig holds optional connection-pool tuning. Zero values use the defaults
// below, which are reasonable for a single-server orchestrator.
type DBConfig struct {
	MaxOpenConns    int           // default: 25
	MaxIdleConns    int           // default: 10
	ConnMaxLifetime time.Duration // default: 5 minutes
	ConnMaxIdleTime time.Duration // default: 2 minutes
}

func (c *DBConfig) applyDefaults() {
	if c.MaxOpenConns == 0 {
		c.MaxOpenConns = 25
	}
	if c.MaxIdleConns == 0 {
		c.MaxIdleConns = 10
	}
	if c.ConnMaxLifetime == 0 {
		c.ConnMaxLifetime = 5 * time.Minute
	}
	if c.ConnMaxIdleTime == 0 {
		c.ConnMaxIdleTime = 2 * time.Minute
	}
}

// New opens a PostgreSQL connection, applies pool config, and returns a Storage
// containing all repository implementations.
func New(dsn string, cfg ...DBConfig) (*Storage, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("persistence: open db: %w", err)
	}

	// Apply pool settings.
	poolCfg := DBConfig{}
	if len(cfg) > 0 {
		poolCfg = cfg[0]
	}
	poolCfg.applyDefaults()

	db.SetMaxOpenConns(poolCfg.MaxOpenConns)
	db.SetMaxIdleConns(poolCfg.MaxIdleConns)
	db.SetConnMaxLifetime(poolCfg.ConnMaxLifetime)
	db.SetConnMaxIdleTime(poolCfg.ConnMaxIdleTime)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("persistence: ping db: %w", err)
	}

	return &Storage{
		Namespace:              &PostgresNamespaceRepository{db: db},
		WorkflowDefinition:     &PostgresWorkflowDefinitionRepository{db: db},
		WorkflowDefinitionStep: &PostgresWorkflowDefinitionStepRepository{db: db},
		WorkflowExecution:      &PostgresWorkflowExecutionRepository{db: db},
		Task:                   &PostgresTaskRepository{db: db},
		HistoryEvent:           &PostgresHistoryEventRepository{db: db},
		Checkpoint:             &PostgresCheckpointRepository{db: db},
		CompensationTask:       &PostgresCompensationTaskRepository{db: db},
		TxManager: 				&PostgresTransactionManager{db: db},
	}, nil
}
//------------------------------helper tx manager-------------------------------------------

type txKey struct{}

type queryer interface {
	QueryRowContext(ctx context.Context,query string, arg ...interface{}) *sql.Row
	ExecContext(ctx context.Context, query string, arg ...interface{}) (sql.Result,error)
	QueryContext(ctx context.Context, query string, arg ...interface{}) (*sql.Rows, error)
}

func getDB(ctx context.Context, fallback *sql.DB) queryer {
	if tx, ok := ctx.Value(txKey{}).(*sql.Tx); ok {
		return tx
	}
	return fallback
}


