package model

// BlockPublication is a durable, transaction-backed count of rows inserted for
// a finalized block. It is retained so Redis publication can be retried after a
// crash or a lost reply without counting a block twice.
type BlockPublication struct {
	BlockNum       uint  `gorm:"primaryKey;autoIncrement:false"`
	ExtrinsicCount int64 `gorm:"not null"`
	SignedCount    int64 `gorm:"not null"`
}

func (BlockPublication) TableName() string { return "block_publications" }
