package dao

import (
	"context"
	"errors"
	"github.com/itering/subscan-plugin/storage"
	"github.com/itering/subscan/model"
	bModel "github.com/itering/subscan/plugins/balance/model"
	"gorm.io/gorm"
)

func CreateTransfer(ctx context.Context, d *Storage, transfer *bModel.Transfer) error {
	if err := withCounts(ctx, d, func(tx *gorm.DB, counts *bModel.ProjectionCounts) error {
		query := tx.Scopes(model.IgnoreDuplicate).Create(transfer)
		if query.Error != nil {
			return query.Error
		}
		counts.Transfers += query.RowsAffected
		return nil
	}); err != nil {
		return err
	}
	// Retry all subsequent effects even when the transfer already exists.
	if err := SyncCounts(ctx, d); err != nil {
		return err
	}
	senderErr := RefreshAccount(ctx, d, model.CheckoutParamValueAddress(transfer.Sender))
	receiverErr := RefreshAccount(ctx, d, model.CheckoutParamValueAddress(transfer.Receiver))
	return errors.Join(senderErr, receiverErr)
}

func TransfersCursor(ctx context.Context, db storage.DB, limit int, before, after *uint, opts ...model.Option) ([]bModel.Transfer, bool, bool) {
	var list []bModel.Transfer
	d := db.GetDbInstance().(*gorm.DB)
	fetch := limit + 1
	q := d.WithContext(ctx).Model(bModel.Transfer{}).Scopes(opts...)
	var hasPrev, hasNext bool
	if after != nil && *after > 0 {
		q = q.Where("id < ?", *after).Order("id desc")
	} else if before != nil && *before > 0 {
		q = q.Where("id > ?", *before).Order("id asc")
	} else {
		q = q.Order("id desc")
	}
	q = q.Limit(fetch).Find(&list)
	if q.Error != nil {
		return nil, false, false
	}
	if before != nil && *before > 0 {
		hasPrev = len(list) > limit
		if hasPrev {
			list = list[:limit]
		}
		for i, j := 0, len(list)-1; i < j; i, j = i+1, j-1 {
			list[i], list[j] = list[j], list[i]
		}
		hasNext = true
	} else {
		hasNext = len(list) > limit
		if hasNext {
			list = list[:limit]
		}
		hasPrev = after != nil && *after > 0
	}
	return list, hasPrev, hasNext
}
