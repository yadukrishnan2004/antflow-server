package usecase

import (
	"context"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (i *workflowInteractor) GetHistoryAfter(ctx context.Context, workflowID string, afterID int64) ([]workflow.HistoryEvent, error) {
	return i.historyRepo.GetByExecutionAfter(ctx, workflowID, afterID)
}