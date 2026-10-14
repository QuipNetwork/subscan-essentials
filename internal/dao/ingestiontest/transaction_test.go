// Package ingestiontest tests real storage without the legacy dao test init,
// which connects to and truncates its configured database before test selection.
package ingestiontest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/itering/subscan/configs"
	"github.com/itering/subscan/internal/dao"
	"github.com/itering/subscan/model"
	"github.com/itering/subscan/util"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type lostExecReply struct {
	redis.Conn
	lose *atomic.Bool
}

func (c lostExecReply) Do(command string, args ...interface{}) (interface{}, error) {
	v, err := c.Conn.Do(command, args...)
	if command == "EXEC" && err == nil && c.lose.CompareAndSwap(true, false) {
		return nil, errors.New("injected lost EXEC reply")
	}
	return v, err
}

func TestTransactionPublication(t *testing.T) {
	dsn, redisAddr := os.Getenv("INGESTION_TEST_POSTGRES"), os.Getenv("INGESTION_TEST_REDIS")
	if dsn == "" || redisAddr == "" {
		t.Skip("run scripts/test-ingestion-transactions.sh for disposable PostgreSQL/Redis tests")
	}
	require.True(t, strings.HasPrefix(util.NetworkNode, "ingestion_test_"), "use an isolated NETWORK_NODE")
	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	schema := fmt.Sprintf("ingestion_test_%d", time.Now().UnixNano())
	require.NoError(t, admin.Exec("CREATE SCHEMA "+schema).Error)
	t.Cleanup(func() {
		_ = admin.Exec("DROP SCHEMA " + schema + " CASCADE").Error
		conn, _ := admin.DB()
		_ = conn.Close()
	})
	t.Setenv("DB_DRIVER", "postgres")
	t.Setenv("MYSQL_MIGRATE", "true")
	configs.Boot.Database = &configs.Database{Postgres: &configs.Postgres{DSN: dsn + " search_path=" + schema}}
	configs.Boot.Redis = &configs.Redis{Addr: redisAddr}
	d, storage, pool := dao.New()
	db := storage.GetDbInstance().(*gorm.DB)
	// Prevent CreateBlock's unrelated next-partition migration goroutine.
	d.AddIndex(model.SplitTableBlockNum)
	t.Cleanup(func() { d.Close(); conn, _ := db.DB(); _ = conn.Close() })
	conn, err := redis.Dial("tcp", redisAddr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	ledger := model.RedisKeyPrefix() + "block_publications"
	keys := []interface{}{dao.RedisMetadataKey, dao.RedisFillAlreadyBlockNum, dao.RedisFillFinalizedBlockNum, ledger}
	t.Cleanup(func() { _, _ = conn.Do("DEL", keys...) })
	ctx := context.Background()
	rows := []model.ChainExtrinsic{
		{ID: 4200000, BlockNum: 42, ExtrinsicIndex: "42-0"},
		{ID: 4200001, BlockNum: 42, ExtrinsicIndex: "42-1", IsSigned: true, AccountId: strings.Repeat("a", 64)},
		{ID: 4200002, BlockNum: 42, ExtrinsicIndex: "42-2", IsSigned: true},
	}
	insert := func(t *testing.T) *dao.GormDB {
		t.Helper()
		tx, err := d.DbBegin()
		require.NoError(t, err)
		t.Cleanup(func() { d.DbRollback(tx) })
		require.NoError(t, d.CreateExtrinsic(ctx, tx, rows, 999), "signed counts must come from inserted rows")
		require.NoError(t, d.CreateEvent(tx, []model.ChainEvent{{ID: 4200000, BlockNum: 42, EventId: "Ready", ModuleId: "example"}}))
		require.NoError(t, d.CreateLog(tx, []model.ChainLog{{ID: 4200000, BlockNum: 42, LogIndex: "42-0", LogType: "Other"}}))
		require.NoError(t, d.CreateBlock(ctx, tx, &model.ChainBlock{ID: 42, BlockNum: 42, Hash: "hash42", Finalized: true, ExtrinsicsCount: len(rows)}))
		return tx
	}
	assertNoRows := func(t *testing.T) {
		t.Helper()
		for _, row := range []interface{}{&model.ChainBlock{}, &model.ChainEvent{}, &model.ChainExtrinsic{}, &model.ChainLog{}, &model.BlockPublication{}, &model.AccountExtrinsicMapping{}} {
			var count int64
			require.NoError(t, db.Model(row).Count(&count).Error)
			require.Zero(t, count)
		}
		v, err := redis.Int(conn.Do("EXISTS", keys...))
		require.NoError(t, err)
		require.Zero(t, v)
	}
	assertCounts := func(t *testing.T, total, signed int64, progress int) {
		t.Helper()
		values, err := redis.StringMap(conn.Do("HGETALL", dao.RedisMetadataKey))
		require.NoError(t, err)
		require.Equal(t, fmt.Sprint(total), values["count_extrinsic"])
		require.Equal(t, fmt.Sprint(signed), values["count_signed_extrinsic"])
		for _, key := range []string{dao.RedisFillAlreadyBlockNum, dao.RedisFillFinalizedBlockNum} {
			n, err := redis.Int(conn.Do("GET", key))
			require.NoError(t, err)
			require.Equal(t, progress, n)
		}
	}

	t.Run("rollback and statement failure", func(t *testing.T) {
		tx := insert(t)
		require.Error(t, d.PublishBlockCompletion(ctx, 42), "uncommitted rows must be invisible")
		require.Error(t, tx.Exec("SELECT * FROM intentionally_missing_table").Error)
		d.DbRollback(tx)
		assertNoRows(t)
	})
	t.Run("actual commit failure", func(t *testing.T) {
		require.NoError(t, db.Exec("CREATE TABLE commit_parent (id integer PRIMARY KEY)").Error)
		require.NoError(t, db.Exec("CREATE TABLE commit_child (id integer REFERENCES commit_parent(id) DEFERRABLE INITIALLY DEFERRED)").Error)
		tx := insert(t)
		require.NoError(t, tx.Exec("INSERT INTO commit_child VALUES (123)").Error)
		require.Error(t, d.DbCommit(tx), "deferred foreign key fails at COMMIT")
		require.ErrorIs(t, d.DbCommit(tx), sql.ErrTxDone)
		d.DbRollback(tx)
		assertNoRows(t)
	})
	t.Run("retry and publication failure", func(t *testing.T) {
		tx := insert(t)
		require.NoError(t, d.DbCommit(tx))
		d.DbRollback(tx)
		var receipt model.BlockPublication
		require.NoError(t, db.First(&receipt, "block_num = ?", 42).Error)
		require.Equal(t, int64(3), receipt.ExtrinsicCount)
		require.Equal(t, int64(2), receipt.SignedCount)
		_, err := conn.Do("SET", dao.RedisFillFinalizedBlockNum, "invalid")
		require.NoError(t, err)
		require.Error(t, d.PublishBlockCompletion(ctx, 42))
		n, err := redis.Int(conn.Do("EXISTS", dao.RedisMetadataKey, ledger, dao.RedisFillAlreadyBlockNum))
		require.NoError(t, err)
		require.Zero(t, n, "validation failure must not partly publish")
		_, err = conn.Do("DEL", dao.RedisFillFinalizedBlockNum)
		require.NoError(t, err)
		require.NoError(t, d.PublishBlockCompletion(ctx, 42))
		require.NoError(t, d.PublishBlockCompletion(ctx, 42))
		assertCounts(t, 3, 2, 42)
		// Full duplicate SQL retry preserves both receipt and Redis counts.
		tx = insert(t)
		require.NoError(t, d.DbCommit(tx))
		require.NoError(t, d.PublishBlockCompletion(ctx, 42))
		assertCounts(t, 3, 2, 42)
	})
	t.Run("partial duplicates and lost Redis reply", func(t *testing.T) {
		rows = append(rows, model.ChainExtrinsic{ID: 4200003, BlockNum: 42, ExtrinsicIndex: "42-3"}, model.ChainExtrinsic{ID: 4200004, BlockNum: 42, ExtrinsicIndex: "42-4", IsSigned: true})
		tx := insert(t)
		require.NoError(t, d.DbCommit(tx))
		var lose atomic.Bool
		lose.Store(true)
		require.NoError(t, pool.Redis().Close())
		*pool.Redis() = redis.Pool{Dial: func() (redis.Conn, error) {
			c, e := redis.Dial("tcp", redisAddr)
			if e != nil {
				return nil, e
			}
			return lostExecReply{Conn: c, lose: &lose}, nil
		}}
		require.ErrorContains(t, d.PublishBlockCompletion(ctx, 42), "lost EXEC reply")
		assertCounts(t, 5, 3, 42)
		require.NoError(t, d.PublishBlockCompletion(ctx, 42))
		assertCounts(t, 5, 3, 42)
		var count int64
		require.NoError(t, db.Model(&model.ChainExtrinsic{}).Count(&count).Error)
		require.Equal(t, int64(5), count)
	})
	t.Run("concurrent retries and out of order progress", func(t *testing.T) {
		tx, err := d.DbBegin()
		require.NoError(t, err)
		require.NoError(t, d.CreateBlock(ctx, tx, &model.ChainBlock{ID: 43, BlockNum: 43, Hash: "hash43", Finalized: true}))
		require.NoError(t, d.DbCommit(tx))
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int) { defer wg.Done(); errs <- d.PublishBlockCompletion(ctx, uint(42+i%2)) }(i)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		assertCounts(t, 5, 3, 43)
	})
	t.Run("multiple batches and exact integer counters", func(t *testing.T) {
		large := make([]model.ChainExtrinsic, 2003)
		for i := range large {
			large[i] = model.ChainExtrinsic{ID: uint(4400000 + i), BlockNum: 44, ExtrinsicIndex: fmt.Sprintf("44-%d", i), IsSigned: true}
		}
		for _, batch := range [][]model.ChainExtrinsic{large[:2001], large} {
			tx, err := d.DbBegin()
			require.NoError(t, err)
			t.Cleanup(func() { d.DbRollback(tx) })
			require.NoError(t, d.CreateExtrinsic(ctx, tx, batch, len(batch)))
			require.NoError(t, d.CreateBlock(ctx, tx, &model.ChainBlock{ID: 44, BlockNum: 44, Hash: "hash44", Finalized: true}))
			require.NoError(t, d.DbCommit(tx))
		}
		_, err := conn.Do("HSET", dao.RedisMetadataKey, "count_extrinsic", math.MaxInt64)
		require.NoError(t, err)
		require.ErrorContains(t, d.PublishBlockCompletion(ctx, 44), "overflow")
		n, err := redis.Int(conn.Do("HEXISTS", ledger, "44:total"))
		require.NoError(t, err)
		require.Zero(t, n)
		progress, err := redis.Int(conn.Do("GET", dao.RedisFillAlreadyBlockNum))
		require.NoError(t, err)
		require.Equal(t, 43, progress)
		const baseline int64 = 9007199254740993 // 2^53+1; cannot round-trip through float64.
		_, err = conn.Do("HSET", dao.RedisMetadataKey, "count_extrinsic", baseline)
		require.NoError(t, err)
		require.NoError(t, d.PublishBlockCompletion(ctx, 44))
		require.NoError(t, d.PublishBlockCompletion(ctx, 44))
		assertCounts(t, baseline+2003, 2006, 44)
		var receipt model.BlockPublication
		require.NoError(t, db.First(&receipt, "block_num = ?", 44).Error)
		require.Equal(t, int64(2003), receipt.ExtrinsicCount)
		require.Equal(t, int64(2003), receipt.SignedCount)
	})
}
