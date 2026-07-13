package persistence

import (
	"context"
)

func (tm *PostgresTransactionManager) RunInTx(ctx context.Context, fn func(ctx context.Context) error )error{
	tx, err:=tm.db.BeginTx(ctx,nil)
	if err != nil {
		return err
	}

	defer func(){
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()

	txCtx :=context.WithValue(ctx,txKey{}, tx)
	if err := fn(txCtx);err!= nil {
		return err
	}
	return tx.Commit()
}