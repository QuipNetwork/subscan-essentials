package dao

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/gomodule/redigo/redis"
	"github.com/itering/subscan/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func recordBlockCounts(ctx context.Context, txn *GormDB, blockNum uint, total, signed int64) error {
	row := model.BlockPublication{BlockNum: blockNum, ExtrinsicCount: total, SignedCount: signed}
	// Insert first, then update: portable across PostgreSQL and MySQL, with the
	// row lock serializing concurrent retries of the same finalized block.
	if err := txn.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&model.BlockPublication{BlockNum: blockNum}).Error; err != nil {
		return err
	}
	return txn.WithContext(ctx).Model(&row).Where("block_num = ?", blockNum).Updates(map[string]interface{}{
		"extrinsic_count": gorm.Expr("extrinsic_count + ?", total),
		"signed_count":    gorm.Expr("signed_count + ?", signed),
	}).Error
}

// PublishBlockCompletion reads committed SQL state only. The Redis transaction
// updates both counters, their per-block deduplication record, and both progress
// markers together. A lost EXEC reply is safe to retry. Redis must retain these
// keys together; cache resets/rebuilds must not discard the ledger alone.
func (d *Dao) PublishBlockCompletion(ctx context.Context, blockNum uint) error {
	var block model.ChainBlock
	if err := d.db.WithContext(ctx).Scopes(d.TableNameFunc(&model.ChainBlock{BlockNum: blockNum})).Where("block_num = ?", blockNum).First(&block).Error; err != nil {
		return err
	}
	if !block.Finalized || block.CodecError {
		return fmt.Errorf("block %d is not successfully finalized", blockNum)
	}
	row := model.BlockPublication{BlockNum: blockNum}
	if err := d.db.WithContext(ctx).Where("block_num = ?", blockNum).First(&row).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	// Pre-migration blocks have no receipt. Their counters were already handled
	// by the old importer; recovering progress must not count them a second time.
	conn, err := d.redis.Redis().GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return publishBlockCompletion(ctx, conn, row)
}

var redisBlockPublications = model.RedisKeyPrefix() + "block_publications"

func publishBlockCompletion(ctx context.Context, conn redis.Conn, row model.BlockPublication) error {
	if row.ExtrinsicCount < 0 || row.SignedCount < 0 || row.SignedCount > row.ExtrinsicCount {
		return errors.New("invalid block publication counts")
	}
	blockNum := strconv.FormatUint(uint64(row.BlockNum), 10)
	for attempt := 0; attempt < 16; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := conn.Do("WATCH", RedisMetadataKey, redisBlockPublications, RedisFillAlreadyBlockNum, RedisFillFinalizedBlockNum); err != nil {
			return err
		}
		// All type/range validation precedes MULTI. Use Go integer arithmetic,
		// not Lua/JSON float64, so counters retain exact int64 precision.
		read := func(command string, args ...interface{}) (int64, error) {
			n, err := redis.Int64(conn.Do(command, args...))
			if errors.Is(err, redis.ErrNil) {
				return 0, nil
			}
			if err == nil && n < 0 {
				err = errors.New("negative ingestion counter/progress")
			}
			return n, err
		}
		commands := [][]interface{}{
			{RedisMetadataKey, "count_extrinsic"}, {RedisMetadataKey, "count_signed_extrinsic"},
			{redisBlockPublications, blockNum + ":total"}, {redisBlockPublications, blockNum + ":signed"},
			{RedisFillAlreadyBlockNum}, {RedisFillFinalizedBlockNum},
		}
		values := make([]int64, len(commands))
		for i, args := range commands {
			command := "HGET"
			if i >= 4 {
				command = "GET"
			}
			n, err := read(command, args...)
			if err != nil {
				return err
			}
			values[i] = n
		}
		// An older publisher can race a forced repair. Never undo an already
		// published increment when its SQL snapshot was taken earlier.
		total, signed := max(row.ExtrinsicCount, values[2]), max(row.SignedCount, values[3])
		deltaTotal, deltaSigned := total-values[2], signed-values[3]
		if values[0] > math.MaxInt64-deltaTotal || values[1] > math.MaxInt64-deltaSigned || uint64(row.BlockNum) > math.MaxInt64 {
			return errors.New("ingestion counter/progress overflow")
		}
		if err := conn.Send("MULTI"); err != nil {
			return err
		}
		writes := []struct {
			command string
			args    []interface{}
		}{
			{"HSET", []interface{}{RedisMetadataKey, "count_extrinsic", values[0] + deltaTotal, "count_signed_extrinsic", values[1] + deltaSigned}},
			{"HSET", []interface{}{redisBlockPublications, blockNum + ":total", total, blockNum + ":signed", signed}},
			{"SET", []interface{}{RedisFillAlreadyBlockNum, max(values[4], int64(row.BlockNum))}},
			{"SET", []interface{}{RedisFillFinalizedBlockNum, max(values[5], int64(row.BlockNum))}},
		}
		for _, w := range writes {
			if err := conn.Send(w.command, w.args...); err != nil {
				return err
			}
		}
		replies, err := redis.Values(conn.Do("EXEC"))
		if errors.Is(err, redis.ErrNil) {
			continue // WATCH conflict; no write executed.
		}
		if err != nil {
			return err
		}
		for _, reply := range replies {
			if err, ok := reply.(redis.Error); ok {
				return err
			}
		}
		return nil
	}
	return errors.New("block publication contention; retry required")
}
