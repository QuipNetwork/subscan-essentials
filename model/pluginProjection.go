package model

import "time"

// PluginProjection is committed with its source block. QueuedAt is delivery
// evidence only; CompletedAt is written only after successful plugin processing.
type PluginProjection struct {
	ID            string `gorm:"primaryKey;size:64" json:"id"`
	BlockNum      uint   `gorm:"index"`
	PluginName    string `gorm:"size:100"`
	Kind          string `gorm:"size:20"`
	EntityIndex   string `gorm:"size:100"`
	NextAttemptAt int64  `gorm:"index:projection_pending,priority:2;not null"`
	QueuedAt      *time.Time
	CompletedAt   *time.Time `gorm:"index:projection_pending,priority:1"`
	LastError     string     `gorm:"type:text"`
}

func (PluginProjection) TableName() string { return "plugin_projections" }
