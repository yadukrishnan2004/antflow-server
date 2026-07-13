package persistence

import (
	"context"
	"database/sql"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

// Migrate creates the workflow_execution table.
//
// New vs old schema:
//   - completed_steps INTEGER NOT NULL DEFAULT 0 — used by INDEPENDENT workflows
//     to atomically track how many steps have finished without querying the task
//     table (which deletes rows on completion).
func (s *PostgresWorkflowExecutionRepository) Migrate() error {
	_, err := s.db.Exec(`
		CREATE EXTENSION IF NOT EXISTS "pgcrypto";

		CREATE TABLE IF NOT EXISTS workflow_execution (
			id                     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
			workflow_definition_id UUID        NOT NULL,
			workflow_name          TEXT        NOT NULL,
			task_queue             TEXT        NOT NULL DEFAULT 'default',
			total_steps            INTEGER     NOT NULL,
			completed_steps        INTEGER     NOT NULL DEFAULT 0,
			workflow_type          TEXT        NOT NULL,

			input       BYTEA,
			result      BYTEA,

			state       TEXT        NOT NULL DEFAULT 'CREATED',
			error       TEXT,

			current_step INTEGER    NOT NULL DEFAULT 0,

			created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			completed_at TIMESTAMPTZ,
			compensation_total    INTEGER     NOT NULL DEFAULT 0,
			compensation_done     INTEGER     NOT NULL DEFAULT 0,

			CONSTRAINT fk_workflow_execution_definition
				FOREIGN KEY (workflow_definition_id)
				REFERENCES workflow_definition(id)
				ON DELETE CASCADE,

			CONSTRAINT chk_workflow_execution_state
				CHECK (state IN ('CREATED','RUNNING','COMPLETED','FAILED','CANCELLED','COMPENSATING')),

			CONSTRAINT chk_workflow_execution_current_step
				CHECK (current_step >= 0),

			CONSTRAINT chk_workflow_execution_completed_steps
				CHECK (completed_steps >= 0)
		);

		ALTER TABLE workflow_execution ADD COLUMN IF NOT EXISTS compensation_total INTEGER NOT NULL DEFAULT 0;
		ALTER TABLE workflow_execution ADD COLUMN IF NOT EXISTS compensation_done INTEGER NOT NULL DEFAULT 0;

		ALTER TABLE workflow_execution DROP CONSTRAINT IF EXISTS chk_workflow_execution_state;
		ALTER TABLE workflow_execution ADD CONSTRAINT chk_workflow_execution_state
			CHECK (state IN ('CREATED','RUNNING','COMPLETED','FAILED','CANCELLED','COMPENSATING'));

	   ALTER TABLE workflow_execution
    ADD COLUMN IF NOT EXISTS deadline_at TIMESTAMPTZ NULL;

CREATE INDEX IF NOT EXISTS idx_workflow_execution_deadline
    ON workflow_execution (deadline_at)
    WHERE deadline_at IS NOT NULL;
	`)
	return err
}

func (s *PostgresWorkflowExecutionRepository) Create(
	ctx context.Context, exec *workflow.WorkflowExecution,
) error {
	return getDB(ctx, s.db).QueryRowContext(ctx, `
    INSERT INTO workflow_execution
        (id, workflow_definition_id, workflow_name, task_queue,
         total_steps, completed_steps, workflow_type,
         input, state, current_step, deadline_at,
         scheduled_at, created_at, updated_at,
         compensation_total, compensation_done)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
    RETURNING created_at, updated_at
`,
    exec.ID, exec.WorkflowDefinitionID, exec.WorkflowName, exec.TaskQueue,
    exec.TotalSteps, exec.CompletedSteps, string(exec.WorkflowType),
    exec.Input, string(exec.State), exec.CurrentStep, exec.DeadlineAt, // NEW
    exec.ScheduledAt, exec.CreatedAt, exec.UpdatedAt,
    exec.CompensationTotal, exec.CompensationDone,
).Scan(&exec.CreatedAt, &exec.UpdatedAt)
}

func (s *PostgresWorkflowExecutionRepository) GetByID(
	ctx context.Context, id string,
) (*workflow.WorkflowExecution, error) {
	exec := &workflow.WorkflowExecution{}
	var completedAt, deadlineAt sql.NullTime
	var stateStr, wfTypeStr string

	err := s.db.QueryRowContext(ctx, `
		SELECT id, workflow_definition_id, input, result,
		       state, COALESCE(error,''), current_step, completed_steps,
		       created_at, scheduled_at, updated_at, completed_at,
		       workflow_name, workflow_type, total_steps, task_queue,
		       compensation_total, compensation_done, deadline_at
		FROM   workflow_execution
		WHERE  id = $1
	`, id).Scan(
		&exec.ID,
		&exec.WorkflowDefinitionID,
		&exec.Input,
		&exec.Result,
		&stateStr,
		&exec.Error,
		&exec.CurrentStep,
		&exec.CompletedSteps,
		&exec.CreatedAt,
		&exec.ScheduledAt,
		&exec.UpdatedAt,
		&completedAt,
		&exec.WorkflowName,
		&wfTypeStr,
		&exec.TotalSteps,
		&exec.TaskQueue,
		&exec.CompensationTotal,
		&exec.CompensationDone,
		&deadlineAt,
	)
	if err == sql.ErrNoRows {
		return nil, workflow.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	exec.State = workflow.State(stateStr)
	exec.WorkflowType = workflow.WorkflowType(wfTypeStr)
	if completedAt.Valid {
		exec.CompletedAt = &completedAt.Time
	}
	if deadlineAt.Valid {
		exec.DeadlineAt = &deadlineAt.Time
	}
	return exec, nil
}

func (s *PostgresWorkflowExecutionRepository) UpdateState(
	ctx context.Context, id string, state workflow.State,
) error {
	var err error
	switch state {
	case workflow.StateCompleted, workflow.StateFailed, workflow.StateCancelled:
		_, err = getDB(ctx, s.db).ExecContext(ctx,
			`UPDATE workflow_execution
			 SET state=$1, completed_at=NOW(), updated_at=NOW()
			 WHERE id=$2 AND state NOT IN ('COMPLETED','FAILED','CANCELLED')`,
			string(state), id)
	default:
		_, err = s.db.ExecContext(ctx,
			`UPDATE workflow_execution
			 SET state=$1, updated_at=NOW()
			 WHERE id=$2 AND state NOT IN ('COMPLETED','FAILED','CANCELLED')`,
			string(state), id)
	}
	return err
}

func (s *PostgresWorkflowExecutionRepository) UpdateStepCursor(
	ctx context.Context, id string, nextStep int,
) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE workflow_execution
		 SET current_step=$1, updated_at=NOW()
		 WHERE id=$2 AND state NOT IN ('COMPLETED','FAILED','CANCELLED')`,
		nextStep, id)
	return err
}

func (s *PostgresWorkflowExecutionRepository) SaveResult(
	ctx context.Context, id string, result []byte,
) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE workflow_execution
		 SET result=$1, updated_at=NOW()
		 WHERE id=$2 AND state NOT IN ('COMPLETED','FAILED','CANCELLED')`,
		result, id)
	return err
}

// IncrementCompletedSteps atomically increments completed_steps and returns
// the new value. This replaces the old CountCompleted approach which raced
// with concurrent task-row deletions.
func (s *PostgresWorkflowExecutionRepository) IncrementCompletedSteps(
	ctx context.Context, id string,
) (newCount int, err error) {
	err = s.db.QueryRowContext(ctx,
		`UPDATE workflow_execution
		 SET    completed_steps = completed_steps + 1, updated_at = NOW()
		 WHERE  id = $1
		 RETURNING completed_steps`,
		id,
	).Scan(&newCount)
	return newCount, err
}

func (s *PostgresWorkflowExecutionRepository) IncrementCompensationDone(
	ctx context.Context, id string,
) (newCount int, err error) {
	err = s.db.QueryRowContext(ctx,
		`UPDATE workflow_execution
		 SET    compensation_done = compensation_done + 1, updated_at = NOW()
		 WHERE  id = $1
		 RETURNING compensation_done`,
		id,
	).Scan(&newCount)
	return newCount, err
}

func (s *PostgresWorkflowExecutionRepository) SetCompensationTotal(
	ctx context.Context, id string, total int,
) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE workflow_execution
		 SET    compensation_total = $1, updated_at = NOW()
		 WHERE  id = $2`,
		total, id)
	return err
}

func (s *PostgresWorkflowExecutionRepository) ExpireOverdue (
	ctx context.Context,
)([]string, error){
	rows, err := s.db.QueryContext(ctx, `
        UPDATE workflow_execution
        SET    state = 'FAILED',
               error = 'workflow exceeded its configured timeout',
               completed_at = NOW(),
               updated_at = NOW()
        WHERE  deadline_at IS NOT NULL
          AND  deadline_at < NOW()
          AND  state IN ('CREATED', 'RUNNING', 'COMPENSATING')
        RETURNING id
    `)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil{
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids,rows.Err()
}

func (s *PostgresWorkflowExecutionRepository) SaveError(ctx context.Context, id string, errMsg string)error{
	_,err := getDB(ctx, s.db).ExecContext(ctx,`
	UPDATE workflow_execution
	SET error=$1, updated_at = NOW()
	WHERE id=$2
	`, errMsg,id)
	return err
}

func (s *PostgresWorkflowExecutionRepository) GetActiveExecutions(ctx context.Context) ([]*workflow.WorkflowExecution,error) {
	rows, err:= s.db.QueryContext(ctx,`
        SELECT id, workflow_definition_id, input, result,
               state, COALESCE(error,''), current_step, completed_steps,
               created_at, scheduled_at, updated_at, completed_at,
               workflow_name, workflow_type, total_steps, task_queue,
               compensation_total, compensation_done, deadline_at
        FROM   workflow_execution
        WHERE  state IN ('RUNNING', 'COMPENSATING')
		`)

		if err != nil {
			return nil,err
		}

		defer rows.Close()

		var execs []*workflow.WorkflowExecution

		for rows.Next() {
			 exec := &workflow.WorkflowExecution{}
			 var completedAt, deadlineAt sql.NullTime
        var stateStr, wfTypeStr string
        err := rows.Scan(
            &exec.ID, &exec.WorkflowDefinitionID, &exec.Input, &exec.Result,
            &stateStr, &exec.Error, &exec.CurrentStep, &exec.CompletedSteps,
            &exec.CreatedAt, &exec.ScheduledAt, &exec.UpdatedAt, &completedAt,
            &exec.WorkflowName, &wfTypeStr, &exec.TotalSteps, &exec.TaskQueue,
            &exec.CompensationTotal, &exec.CompensationDone, &deadlineAt,
        )
        if err != nil {
            return nil, err
        }
        exec.State = workflow.State(stateStr)
        exec.WorkflowType = workflow.WorkflowType(wfTypeStr)
        if completedAt.Valid {
            exec.CompletedAt = &completedAt.Time
        }
        if deadlineAt.Valid {
            exec.DeadlineAt = &deadlineAt.Time
        }
        execs = append(execs, exec)
    }
    return execs, rows.Err()

}

var _ workflow.WorkflowExecutionRepository = (*PostgresWorkflowExecutionRepository)(nil)