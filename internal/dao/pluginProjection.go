package dao

import (
	"context"
	"time"

	"github.com/itering/subscan/model"
	"gorm.io/gorm/clause"
)

const ProjectionRetryDelay = 30 * time.Second

func (d *Dao) CreatePluginProjections(tx *GormDB, rows []model.PluginProjection) error {
	if len(rows) == 0 {
		return nil
	}
	return tx.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(rows, 1000).Error
}

func (d *Dao) PendingPluginProjections(ctx context.Context, blockNum *uint, limit int) ([]model.PluginProjection, error) {
	var rows []model.PluginProjection
	q := d.db.WithContext(ctx).Where("completed_at IS NULL AND next_attempt_at <= ?", time.Now().UnixMilli())
	if blockNum != nil {
		q = q.Where("block_num = ?", *blockNum)
	}
	err := q.Order("next_attempt_at, id").Limit(limit).Find(&rows).Error
	return rows, err
}

// Reserve before enqueue. A crash on either side of the enqueue leaves pending
// work eligible again after the bounded delay. It never acknowledges processing.
func (d *Dao) ReservePluginProjection(ctx context.Context, id string) (bool, error) {
	now := time.Now()
	q := d.db.WithContext(ctx).Model(&model.PluginProjection{}).
		Where("id = ? AND completed_at IS NULL AND next_attempt_at <= ?", id, now.UnixMilli()).
		Update("next_attempt_at", now.Add(ProjectionRetryDelay).UnixMilli())
	return q.RowsAffected == 1, q.Error
}

func (d *Dao) RecordProjectionDelivery(ctx context.Context, id, failure string) error {
	updates := map[string]interface{}{"last_error": failure}
	if failure == "" {
		updates["queued_at"] = time.Now().UTC()
	}
	return d.db.WithContext(ctx).Model(&model.PluginProjection{}).Where("id = ? AND completed_at IS NULL", id).Updates(updates).Error
}

// ProcessPluginProjection acknowledges only successful processing. Plugin
// effects must be idempotent, including concurrent duplicate deliveries. Do not
// hold a SQL transaction/connection across plugin work: callbacks can need their
// own DB connections and external RPC, exhausting a busy worker's pool.
func (d *Dao) ProcessPluginProjection(ctx context.Context, id string, process func(model.PluginProjection) error) error {
	var row model.PluginProjection
	if err := d.db.WithContext(ctx).First(&row, "id = ?", id).Error; err != nil {
		return err
	}
	if row.CompletedAt != nil {
		return nil
	}
	processingErr := process(row)
	updates := map[string]interface{}{"last_error": ""}
	if processingErr != nil {
		updates["last_error"] = processingErr.Error()
		updates["next_attempt_at"] = time.Now().Add(ProjectionRetryDelay).UnixMilli()
	} else {
		updates["completed_at"] = time.Now().UTC()
	}
	// A delayed failed duplicate must never undo a successful acknowledgement.
	if err := d.db.WithContext(ctx).Model(&row).Where("completed_at IS NULL").Updates(updates).Error; err != nil {
		return err
	}
	return processingErr
}
