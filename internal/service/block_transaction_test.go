package service

import (
	"context"
	"errors"
	"testing"

	"github.com/itering/subscan/internal/dao"
	"github.com/itering/subscan/model"
	"github.com/itering/subscan/util/mq"
	"github.com/itering/substrate-api-rpc/metadata"
	"github.com/stretchr/testify/require"
)

var transactionFault = errors.New("injected transaction failure")

type transactionFaultDao struct {
	blockIntegrityDao
	fail         string
	publications int
}

func (d *transactionFaultDao) DbBegin() (*dao.GormDB, error) {
	if d.fail == "begin" {
		return nil, transactionFault
	}
	return d.blockIntegrityDao.DbBegin()
}
func (d *transactionFaultDao) DbCommit(tx *dao.GormDB) error {
	if d.fail == "commit" {
		return transactionFault
	}
	return d.blockIntegrityDao.DbCommit(tx)
}
func (d *transactionFaultDao) DbRollback(tx *dao.GormDB) {
	if !d.committed {
		clear(d.blocks)
		clear(d.events)
		clear(d.extrinsics)
		d.projections = nil
	}
	d.blockIntegrityDao.DbRollback(tx)
}
func (d *transactionFaultDao) CreateExtrinsic(ctx context.Context, tx *dao.GormDB, rows []model.ChainExtrinsic, signed int) error {
	if d.fail == "extrinsics" {
		return transactionFault
	}
	return d.blockIntegrityDao.CreateExtrinsic(ctx, tx, rows, signed)
}
func (d *transactionFaultDao) CreateEvent(tx *dao.GormDB, rows []model.ChainEvent) error {
	if d.fail == "events" {
		return transactionFault
	}
	return d.blockIntegrityDao.CreateEvent(tx, rows)
}
func (d *transactionFaultDao) CreateLog(*dao.GormDB, []model.ChainLog) error {
	if d.fail == "logs" {
		return transactionFault
	}
	return nil
}
func (d *transactionFaultDao) CreateBlock(ctx context.Context, tx *dao.GormDB, row *model.ChainBlock) error {
	if d.fail == "block" {
		return transactionFault
	}
	return d.blockIntegrityDao.CreateBlock(ctx, tx, row)
}
func (d *transactionFaultDao) CreatePluginProjections(tx *dao.GormDB, rows []model.PluginProjection) error {
	if d.fail == "projections" {
		return transactionFault
	}
	return d.blockIntegrityDao.CreatePluginProjections(tx, rows)
}
func (d *transactionFaultDao) PublishBlockCompletion(context.Context, uint) error {
	if !d.committed {
		return errors.New("publication before commit")
	}
	if d.fail == "publication" {
		return transactionFault
	}
	d.publications++
	return nil
}
func (d *transactionFaultDao) GetBlockByNum(_ context.Context, num uint) *model.ChainBlock {
	if row, ok := d.blocks[num]; ok && d.committed {
		return &row
	}
	return nil
}

type failingNotificationQueue struct {
	blockIntegrityQueue
}

func (q *failingNotificationQueue) ForcePublish(a, b string, c interface{}) error {
	if err := q.blockIntegrityQueue.Publish(a, b, c); err != nil {
		return err
	}
	return errors.New("queue unavailable")
}

func TestBlockTransactionCompletion(t *testing.T) {
	const spec = 900003
	block, events, m := integrityInputs(t)
	previous := metadata.RuntimeMetadata[spec]
	metadata.RuntimeMetadata[spec] = m
	t.Cleanup(func() {
		if previous == nil {
			delete(metadata.RuntimeMetadata, spec)
		} else {
			metadata.RuntimeMetadata[spec] = previous
		}
	})
	for _, fail := range []string{"begin", "extrinsics", "events", "logs", "block", "projections", "commit", "publication", "enqueue"} {
		t.Run(fail, func(t *testing.T) {
			d := &transactionFaultDao{blockIntegrityDao: blockIntegrityDao{
				blocks: map[uint]model.ChainBlock{}, events: map[uint]model.ChainEvent{}, extrinsics: map[uint]model.ChainExtrinsic{},
			}, fail: fail}
			s := &Service{dao: d}
			q := &blockIntegrityQueue{dao: &d.blockIntegrityDao}
			oldQueue := mq.Instant
			mq.Instant = q
			if fail == "enqueue" {
				mq.Instant = &failingNotificationQueue{blockIntegrityQueue: *q}
			}
			t.Cleanup(func() { mq.Instant = oldQueue })
			err := s.CreateChainBlock(context.Background(), "hash", block, events, spec, 0)
			if fail == "enqueue" {
				require.NoError(t, err)
				require.Positive(t, mq.Instant.(*failingNotificationQueue).calls)
				require.Equal(t, 1, d.publications, "queue failure must not suppress progress")
			} else {
				require.ErrorIs(t, err, transactionFault)
				require.Zero(t, d.publications)
			}
			if fail != "publication" && fail != "enqueue" {
				require.False(t, d.committed)
				require.Empty(t, d.blocks)
				require.Empty(t, d.events)
				require.Empty(t, d.extrinsics)
				require.Empty(t, d.projections)
				require.Zero(t, q.calls)
				d.fail = ""
				require.NoError(t, s.CreateChainBlock(context.Background(), "hash", block, events, spec, 0))
			} else {
				actionsBefore := len(d.actions)
				if fail == "publication" {
					require.ErrorIs(t, s.FillBlockData(context.Background(), 42, false), transactionFault)
					require.Zero(t, d.publications)
				}
				d.fail = ""
				// No RPC dependency is configured: retry must use the stored block.
				require.NoError(t, s.FillBlockData(context.Background(), 42, false))
				require.Len(t, d.actions, actionsBefore, "retry must not reinsert rows")
			}
			require.Len(t, d.blocks, 1)
			require.Len(t, d.extrinsics, 3)
			require.Len(t, d.events, 3)
			require.Positive(t, d.publications)
		})
	}
}
