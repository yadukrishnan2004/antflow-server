package usecase

import (
	"context"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (i *workflowInteractor) GetHistory(ctx context.Context, workflowID string) ([]workflow.HistoryEvent, error) {
	return i.historyRepo.GetByExecution(ctx, workflowID)
}
