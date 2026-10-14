package ingestiontest

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	ws "github.com/gorilla/websocket"
	"github.com/itering/scale.go/source"
	"github.com/itering/scale.go/types"
	subscanPlugin "github.com/itering/subscan-plugin"
	"github.com/itering/subscan-plugin/storage"
	"github.com/itering/subscan/internal/service"
	"github.com/itering/subscan/model"
	"github.com/itering/subscan/plugins"
	"github.com/itering/subscan/plugins/balance"
	bModel "github.com/itering/subscan/plugins/balance/model"
	"github.com/itering/subscan/share/token"
	"github.com/itering/subscan/util/mq"
	"github.com/itering/substrate-api-rpc/metadata"
	"github.com/itering/substrate-api-rpc/storageKey"
	"github.com/itering/substrate-api-rpc/websocket"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type enqueueFault struct {
	mq.IJob
	fail bool
}

func (q *enqueueFault) ForcePublish(a, b string, c interface{}) error {
	if q.fail {
		return errors.New("injected enqueue failure")
	}
	return q.IJob.ForcePublish(a, b, c)
}

type counterFault struct {
	subscanPlugin.RedisPool
	fail atomic.Bool
	lose atomic.Bool
}

func (p *counterFault) HmSetEx(ctx context.Context, key string, value interface{}, ttl int) error {
	if p.fail.CompareAndSwap(true, false) {
		return errors.New("injected counter failure after SQL insert")
	}
	err := p.RedisPool.HmSetEx(ctx, key, value, ttl)
	if err == nil && p.lose.CompareAndSwap(true, false) {
		return errors.New("injected lost counter reply")
	}
	return err
}

type crashBalance struct {
	*balance.Balance
	crash atomic.Bool
	calls atomic.Int64
}

func (p *crashBalance) ProcessEvent(block *storage.Block, event *storage.Event, fee decimal.Decimal) error {
	p.calls.Add(1)
	if err := p.Balance.ProcessEvent(block, event, fee); err != nil {
		return err
	}
	if p.crash.CompareAndSwap(true, false) {
		panic("injected worker crash after effects before acknowledgement")
	}
	return nil
}

// Only synthetic System.Account responses for the two named test addresses are
// served by this responder; these values do not represent historical chain state.
func accountRPC(t *testing.T, addresses ...string) {
	t.Helper()
	m := metadata.RegNewMetadataType(900100, "0x6d6574610d000400")
	types.RegCustomTypes(map[string]source.TypeStruct{
		"ProjectionAccount":     {Type: "struct", TypeMapping: [][]string{{"nonce", "U32"}, {"refCount", "U32"}, {"data", "ProjectionAccountData"}}},
		"ProjectionAccountData": {Type: "struct", TypeMapping: [][]string{{"free", "U128"}, {"reserved", "U128"}, {"miscFrozen", "U128"}, {"feeFrozen", "U128"}}},
	})
	m.Metadata.Modules = []types.MetadataModules{{Name: "System", Prefix: "System", Storage: []types.MetadataStorage{{Name: "Account", Type: types.StorageType{Origin: "MapType", MapType: &types.MapType{Hasher: "Blake2_128Concat", Key: "AccountId", Value: "ProjectionAccount"}}}}}}
	allowed := map[string]bool{}
	for _, a := range addresses {
		allowed["0x"+storageKey.EncodeStorageKey("System", "Account", a).EncodeKey] = true
	}
	raw := make([]byte, 72)
	binary.LittleEndian.PutUint32(raw, 2)
	binary.LittleEndian.PutUint64(raw[8:], 100)
	var unexpected atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&ws.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			var req struct {
				ID     interface{} `json:"id"`
				Method string      `json:"method"`
				Params []string    `json:"params"`
			}
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			response := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
			if req.Method != "state_getStorage" || len(req.Params) != 1 || !allowed[req.Params[0]] {
				unexpected.Add(1)
				response["error"] = map[string]interface{}{"code": -32601, "message": "unexpected synthetic RPC request"}
			} else {
				response["result"] = "0x" + hex.EncodeToString(raw)
			}
			if err := conn.WriteJSON(response); err != nil {
				return
			}
		}
	}))
	websocket.Close()
	websocket.SetEndpoint("ws" + strings.TrimPrefix(server.URL, "http"))
	t.Cleanup(func() { websocket.Close(); server.Close(); require.Zero(t, unexpected.Load()) })
}

func TestPluginProjectionRecovery(t *testing.T) {
	d, storageDAO, pool, conn := storageFixture(t)
	db := storageDAO.GetDbInstance().(*gorm.DB)
	ctx := context.Background()
	from, to := strings.Repeat("a", 64), strings.Repeat("b", 64)
	accountRPC(t, from, to)
	oldToken := token.GetDefaultToken()
	token.SetDefault(&token.Token{Symbol: "TEST", TokenId: "TEST", Decimals: 0})
	t.Cleanup(func() { token.SetDefault(oldToken) })
	plugin := &crashBalance{Balance: balance.New()}
	storageDAO.SetPrefix("balance")
	plugin.InitDao(storageDAO)
	counters := &counterFault{RedisPool: pool}
	plugin.SetRedisPool(counters)
	oldPlugins := plugins.RegisteredPlugins
	plugins.RegisteredPlugins = map[string]plugins.PluginFactory{"balance": plugin}
	t.Cleanup(func() { plugins.RegisteredPlugins = oldPlugins })
	oldQueue := mq.Instant
	queue := &enqueueFault{IJob: mq.New(), fail: true}
	t.Cleanup(func() { mq.Instant = oldQueue })
	queueKey := model.RedisKeyPrefix() + "queue:plugin-projection"
	t.Cleanup(func() { _, _ = conn.Do("DEL", queueKey, model.RedisKeyPrefix()+"queues") })
	event := model.ChainEvent{ID: 4200000, BlockNum: 42, EventIdx: 0, ExtrinsicIndex: "42-0", ModuleId: "Balances", EventId: "Transfer", Params: model.EventParams{{Type: "AccountId", Value: from}, {Type: "AccountId", Value: to}, {Type: "Balance", Value: "7"}}}
	tasks := service.BuildPluginProjections(42, []model.ChainEvent{event}, nil)
	require.Len(t, tasks, 2)
	var eventTask model.PluginProjection
	for _, task := range tasks {
		if task.Kind == "event" {
			eventTask = task
		}
	}
	require.NotEmpty(t, eventTask.ID)
	tx, err := d.DbBegin()
	require.NoError(t, err)
	t.Cleanup(func() { d.DbRollback(tx) })
	require.NoError(t, d.CreateEvent(tx, []model.ChainEvent{event}))
	require.NoError(t, d.CreateExtrinsic(ctx, tx, []model.ChainExtrinsic{{ID: 4200000, BlockNum: 42, ExtrinsicIndex: "42-0", CallModule: "Balances", CallModuleFunction: "transfer", Success: true}}, 0))
	require.NoError(t, d.CreateBlock(ctx, tx, &model.ChainBlock{ID: 42, BlockNum: 42, Hash: "hash42", Finalized: true, ExtrinsicsCount: 1, EventCount: 1}))
	require.NoError(t, d.CreatePluginProjections(tx, tasks))
	pending, err := d.PendingPluginProjections(ctx, nil, 256)
	require.NoError(t, err)
	require.Empty(t, pending, "uncommitted work must not be delivered")
	require.NoError(t, d.DbCommit(tx))
	require.NoError(t, d.PublishBlockCompletion(ctx, 42))
	readTask := func() model.PluginProjection {
		var row model.PluginProjection
		require.NoError(t, db.First(&row, "id = ?", eventTask.ID).Error)
		return row
	}
	due := func() {
		require.NoError(t, db.Model(&model.PluginProjection{}).Where("completed_at IS NULL").Update("next_attempt_at", 0).Error)
	}
	deliver := func() string {
		for {
			raw, err := redis.Bytes(conn.Do("RPOP", queueKey))
			require.NoError(t, err)
			var msg struct {
				Args struct {
					ID string `json:"projection_id"`
				} `json:"args"`
			}
			require.NoError(t, json.Unmarshal(raw, &msg))
			require.NotEmpty(t, msg.Args.ID)
			if msg.Args.ID == eventTask.ID {
				return msg.Args.ID
			}
			require.NoError(t, service.ProcessPluginProjection(ctx, d, msg.Args.ID))
		}
	}
	assertCounts := func(transfers, accounts string) {
		values, err := redis.StringMap(conn.Do("HGETALL", model.MetadataCacheKey()))
		require.NoError(t, err)
		require.Equal(t, transfers, values["total_transfer"])
		require.Equal(t, accounts, values["total_account"])
	}

	require.ErrorContains(t, service.DispatchPluginProjections(ctx, d, queue, nil), "enqueue failure")
	require.Nil(t, readTask().QueuedAt)
	require.Nil(t, readTask().CompletedAt)
	progress, err := redis.Int(conn.Do("GET", daoProgressKey()))
	require.NoError(t, err)
	require.Equal(t, 42, progress, "core completion independent of pending projections")
	queue.fail = false
	due()
	require.NoError(t, service.DispatchPluginProjections(ctx, d, queue, nil))
	require.NotNil(t, readTask().QueuedAt)
	require.Nil(t, readTask().CompletedAt)
	_ = deliver() // simulate worker death after delivery, before processing
	due()
	require.NoError(t, service.DispatchPluginProjections(ctx, d, queue, nil))
	id := deliver()
	counters.fail.Store(true)
	require.ErrorContains(t, service.ProcessPluginProjection(ctx, d, id), "counter failure")
	var transferCount int64
	require.NoError(t, db.Model(&bModel.Transfer{}).Count(&transferCount).Error)
	require.Equal(t, int64(1), transferCount)
	require.Nil(t, readTask().CompletedAt, "partial effects are not completed processing")
	due()
	require.NoError(t, service.DispatchPluginProjections(ctx, d, queue, nil))
	id = deliver()
	counters.lose.Store(true)
	require.ErrorContains(t, service.ProcessPluginProjection(ctx, d, id), "lost counter reply")
	require.Nil(t, readTask().CompletedAt)
	assertCounts("1", "0")
	due()
	require.NoError(t, service.DispatchPluginProjections(ctx, d, queue, nil))
	id = deliver()
	plugin.crash.Store(true)
	require.Panics(t, func() { _ = service.ProcessPluginProjection(ctx, d, id) })
	require.Nil(t, readTask().CompletedAt, "crash before acknowledgement leaves work pending")
	assertCounts("1", "2")
	due()
	require.NoError(t, service.DispatchPluginProjections(ctx, d, queue, nil))
	id = deliver()
	// Acknowledgement can fail even after all plugin effects succeed.
	require.NoError(t, db.Exec(`CREATE FUNCTION reject_projection_ack() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.completed_at IS NOT NULL THEN RAISE EXCEPTION 'injected acknowledgement failure'; END IF; RETURN NEW; END $$`).Error)
	require.NoError(t, db.Exec(`CREATE TRIGGER reject_projection_ack BEFORE UPDATE ON plugin_projections FOR EACH ROW EXECUTE FUNCTION reject_projection_ack()`).Error)
	require.ErrorContains(t, service.ProcessPluginProjection(ctx, d, id), "acknowledgement failure")
	require.Nil(t, readTask().CompletedAt)
	assertCounts("1", "2")
	require.NoError(t, db.Exec("DROP TRIGGER reject_projection_ack ON plugin_projections").Error)
	before := plugin.calls.Load()
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- service.ProcessPluginProjection(ctx, d, id) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.NotNil(t, readTask().CompletedAt)
	require.GreaterOrEqual(t, plugin.calls.Load(), before+1, "pending duplicates may process concurrently; their side effects must converge")
	completedCalls := plugin.calls.Load()
	require.NoError(t, service.ProcessPluginProjection(ctx, d, id))
	require.Equal(t, completedCalls, plugin.calls.Load(), "completed duplicate delivery is a no-op")
	retryTx, err := d.DbBegin()
	require.NoError(t, err)
	require.NoError(t, d.CreatePluginProjections(retryTx, tasks))
	require.NoError(t, d.DbCommit(retryTx))
	require.NotNil(t, readTask().CompletedAt, "repeated ingestion must not reset acknowledgement")
	assertCounts("1", "2")
	require.NoError(t, db.Model(&bModel.Transfer{}).Count(&transferCount).Error)
	require.Equal(t, int64(1), transferCount)
	var accounts []bModel.Account
	require.NoError(t, db.Find(&accounts).Error)
	require.Len(t, accounts, 2)
	for _, account := range accounts {
		require.Equal(t, "100", account.Balance.String())
		require.Equal(t, 2, account.Nonce)
	}
	// Exercise the actual balance HTTP handler, not a hand-built response.
	handled := false
	for _, route := range plugin.InitHttp() {
		if route.Router == "transfer" {
			handled = true
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, "/api/plugin/balance/transfer", strings.NewReader(`{"row":10}`))
			require.NoError(t, route.Handle(w, r))
			var response struct {
				Code int `json:"code"`
				Data struct {
					List []bModel.Transfer `json:"list"`
				} `json:"data"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
			require.Zero(t, response.Code)
			require.Len(t, response.Data.List, 1)
			require.Equal(t, uint(4200000), response.Data.List[0].Id)
			require.Equal(t, "7", response.Data.List[0].Amount.String())
		}
	}
	require.True(t, handled, "balance transfer HTTP route must be exercised")

	// A late failing duplicate must not clear another worker's success.
	race := model.PluginProjection{ID: "ack-race", BlockNum: 42, PluginName: "balance", Kind: "block", EntityIndex: "42"}
	require.NoError(t, db.Create(&race).Error)
	started, release := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- d.ProcessPluginProjection(ctx, race.ID, func(model.PluginProjection) error {
			close(started)
			<-release
			return errors.New("late duplicate failure")
		})
	}()
	select {
	case <-started:
	case err := <-result:
		t.Fatalf("worker exited before processing: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start")
	}
	ackErr := d.ProcessPluginProjection(ctx, race.ID, func(model.PluginProjection) error { return nil })
	close(release)
	require.NoError(t, ackErr)
	require.ErrorContains(t, <-result, "late duplicate failure")
	require.NoError(t, db.First(&race, "id = ?", race.ID).Error)
	require.NotNil(t, race.CompletedAt)
	require.Empty(t, race.LastError)
}

func daoProgressKey() string { return model.RedisKeyPrefix() + "FillAlreadyBlockNum" }
