package ingestiontest

import (
	"context"
	"fmt"
	"testing"

	"github.com/itering/subscan/model"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestBlockIDsPreserveGenesis(t *testing.T) {
	for _, tc := range []struct {
		name    string
		heights []uint
	}{
		{"genesis first", []uint{0, 1, 2, 0, 1, 2}},
		{"genesis after block one", []uint{1, 0, 2, 0, 1, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, storage, _, _ := storageFixture(t)
			ctx := context.Background()
			for _, height := range tc.heights {
				tx, err := d.DbBegin()
				require.NoError(t, err)
				t.Cleanup(func() { d.DbRollback(tx) })
				block := &model.ChainBlock{
					ID: height, BlockNum: height, Hash: fmt.Sprintf("hash%d", height), Finalized: true,
				}
				require.NoError(t, d.CreateBlock(ctx, tx, block))
				require.NoError(t, d.DbCommit(tx))
				require.NoError(t, d.PublishBlockCompletion(ctx, height), "block %d must exist after commit", height)
			}

			db := storage.GetDbInstance().(*gorm.DB)
			var blocks []model.ChainBlock
			require.NoError(t, db.Order("block_num").Find(&blocks).Error)
			require.Len(t, blocks, 3, "duplicate jobs must not add or suppress blocks")
			for height, block := range blocks {
				require.Equal(t, uint(height), block.ID)
				require.Equal(t, uint(height), block.BlockNum)
				require.Equal(t, fmt.Sprintf("hash%d", height), block.Hash)
			}
			var receipts int64
			require.NoError(t, db.Model(&model.BlockPublication{}).Count(&receipts).Error)
			require.EqualValues(t, 3, receipts)
		})
	}
}
