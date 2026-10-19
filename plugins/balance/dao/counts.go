package dao

import (
	"context"

	core "github.com/itering/subscan/model"
	bModel "github.com/itering/subscan/plugins/balance/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// withCounts serializes balance row insertion and its SQL counters. Initial
// counts are seeded from existing rows once, so duplicates of pre-upgrade data
// cannot inflate totals. Neither retries nor Redis availability affect them.
func withCounts(ctx context.Context, s *Storage, write func(*gorm.DB, *bModel.ProjectionCounts) error) error {
	db := s.Dao.GetDbInstance().(*gorm.DB)
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&bModel.ProjectionCounts{ID: 1}).Error; err != nil {
			return err
		}
		var counts bModel.ProjectionCounts
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&counts, 1).Error; err != nil {
			return err
		}
		if !counts.Initialized {
			if err := tx.Model(&bModel.Transfer{}).Count(&counts.Transfers).Error; err != nil {
				return err
			}
			if err := tx.Model(&bModel.Account{}).Count(&counts.Accounts).Error; err != nil {
				return err
			}
			counts.Initialized = true
		}
		if err := write(tx, &counts); err != nil {
			return err
		}
		return tx.Save(&counts).Error
	})
}

// SyncCounts writes absolute durable counts, never increments. Holding the SQL
// count-row lock until Redis responds prevents an older publisher from replacing
// newer totals. A lost reply or crash before projection acknowledgement is safe
// to replay. Redis failure propagates and leaves the projection pending.
func SyncCounts(ctx context.Context, s *Storage) error {
	return withCounts(ctx, s, func(_ *gorm.DB, counts *bModel.ProjectionCounts) error {
		return s.Pool.HmSetEx(ctx, core.MetadataCacheKey(), map[string]int64{
			"total_transfer": counts.Transfers, "total_account": counts.Accounts,
		}, -1)
	})
}
