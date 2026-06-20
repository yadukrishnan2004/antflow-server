package usecase

import (
	"context"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (i *workflowInteractor) GetHistory(ctx context.Context, workflowID string) ([]workflow.HistoryEvent, error) {
	return i.historyRepo.GetByExecution(ctx, workflowID)
}

func (i *workflowInteractor) GetHistoryAfter(ctx context.Context, workflowID string, afterID int64) ([]workflow.HistoryEvent, error) {
	return i.historyRepo.GetByExecutionAfter(ctx, workflowID, afterID)
}