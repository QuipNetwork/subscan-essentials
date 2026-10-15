package ingestiontest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/itering/subscan/internal/service"
	"github.com/itering/subscan/model"
	"github.com/itering/subscan/plugins"
	"github.com/itering/subscan/plugins/balance"
	bDao "github.com/itering/subscan/plugins/balance/dao"
	bModel "github.com/itering/subscan/plugins/balance/model"
	"github.com/itering/subscan/util/address"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestAccountStatePersistence(t *testing.T) {
	_, storageDAO, pool, _ := storageFixture(t)
	ctx := context.Background()
	db := storageDAO.GetDbInstance().(*gorm.DB)
	storageDAO.SetPrefix("balance")
	plugin := balance.New()
	plugin.InitDao(storageDAO)
	plugin.SetRedisPool(pool)
	s := &bDao.Storage{Dao: storageDAO, Pool: pool}
	account := strings.Repeat("ab", 32)
	snapshot := func(height uint, status string) bModel.AccountState {
		hash := "0x" + strings.Repeat("00", 31) + string("0123456789abcdef"[height/16]) + string("0123456789abcdef"[height%16])
		out := bModel.AccountState{StateStatus: status, StateHash: hash, StateBlockNum: &height}
		if status == "available" {
			zero := decimal.Zero
			n := uint32(0)
			out.Nonce = &n
			out.Balance = &zero
			out.Locked = &zero
			out.Reserved = &zero
		} else if status == "unknown" {
			out.StateError = "historical state unavailable"
		}
		return out
	}
	read := func() bModel.Account {
		var a bModel.Account
		require.NoError(t, db.First(&a, "address = ?", account).Error)
		return a
	}
	persist := func(state bModel.AccountState) { require.NoError(t, bDao.PersistAccountState(ctx, s, account, state)) }
	// Existing numeric values without availability provenance stay unknown in JSON.
	require.NoError(t, db.Exec("INSERT INTO balance_accounts (address,nonce,balance,locked,reserved) VALUES (?,9,123,4,5)", account).Error)
	a := read()
	raw, err := json.Marshal(a)
	require.NoError(t, err)
	require.Contains(t, string(raw), `"state_status":"unknown"`)
	require.Contains(t, string(raw), `"balance":null`)
	checkHTTP := func(expectedStatus, expectedBalance string) {
		handled := 0
		for _, route := range plugin.InitHttp() {
			if route.Router != "account" && route.Router != "accounts" {
				continue
			}
			payload := `{"row":10}`
			if route.Router == "account" {
				payload = `{"address":"` + address.Encode(account) + `"}`
			}
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, "/api/plugin/balance/"+route.Router, strings.NewReader(payload))
			require.NoError(t, route.Handle(w, r))
			require.Contains(t, w.Body.String(), `"state_status":"`+expectedStatus+`"`)
			require.Contains(t, w.Body.String(), `"balance":`+expectedBalance)
			handled++
		}
		require.Equal(t, 2, handled)
	}
	persist(snapshot(42, "available"))
	checkHTTP("available", `"0"`)
	a = read()
	require.Equal(t, "0", a.Balance.String())
	require.Zero(t, *a.Nonce)
	persist(snapshot(42, "unknown"))
	require.Equal(t, "available", read().StateStatus, "same-hash failure cannot erase known state")
	persist(snapshot(41, "unknown"))
	require.Equal(t, uint(42), *read().StateBlockNum)
	var old bModel.AccountObservation
	require.NoError(t, db.First(&old, "address = ? AND observation_hash = ?", account, snapshot(41, "unknown").StateHash).Error)
	require.Equal(t, "unknown", old.StateStatus)
	persist(snapshot(43, "unknown"))
	a = read()
	require.Equal(t, "unknown", a.StateStatus)
	require.Nil(t, a.Balance)
	require.Nil(t, a.Nonce)
	persist(snapshot(42, "available"))
	require.Equal(t, uint(43), *read().StateBlockNum)
	persist(snapshot(43, "available"))
	require.Equal(t, "available", read().StateStatus, "same-hash explicit replay can recover unavailable state")
	persist(snapshot(44, "absent"))
	require.Equal(t, "absent", read().StateStatus)
	require.Nil(t, read().Balance)
	// Concurrent/out-of-order retries converge to the highest observed finalized block.
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for _, height := range []uint{45, 50, 46, 47, 49, 48, 50, 46} {
		wg.Add(1)
		go func(n uint) {
			defer wg.Done()
			errs <- bDao.PersistAccountState(ctx, s, account, snapshot(n, "available"))
		}(height)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, uint(50), *read().StateBlockNum)
	var counts bModel.ProjectionCounts
	require.NoError(t, db.First(&counts, 1).Error)
	require.Equal(t, int64(1), counts.Accounts)
	// A database write failure rolls back both observation and current view.
	require.NoError(t, db.Exec(`CREATE FUNCTION reject_account_observation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected observation failure'; END $$`).Error)
	require.NoError(t, db.Exec(`CREATE TRIGGER reject_account_observation BEFORE INSERT ON balance_account_observations FOR EACH ROW EXECUTE FUNCTION reject_account_observation()`).Error)
	require.ErrorContains(t, bDao.PersistAccountState(ctx, s, account, snapshot(51, "unknown")), "observation failure")
	require.Equal(t, uint(50), *read().StateBlockNum)
	require.NoError(t, db.Exec("DROP TRIGGER reject_account_observation ON balance_account_observations").Error)
	persist(snapshot(51, "unknown"))
	checkHTTP("unknown", `null`)
}

func TestUnknownAccountProjectionCompletes(t *testing.T) {
	d, storageDAO, pool, _ := storageFixture(t)
	db := storageDAO.GetDbInstance().(*gorm.DB)
	ctx := context.Background()
	account := strings.Repeat("ab", 32)
	unavailable := accountRPC(t, account)
	unavailable.Store(true)
	storageDAO.SetPrefix("balance")
	plugin := balance.New()
	plugin.InitDao(storageDAO)
	plugin.SetRedisPool(pool)
	old := plugins.RegisteredPlugins
	plugins.RegisteredPlugins = map[string]plugins.PluginFactory{"balance": plugin}
	t.Cleanup(func() { plugins.RegisteredPlugins = old })
	block := model.ChainBlock{ID: 42, BlockNum: 42, Hash: "0x" + strings.Repeat("42", 32), Finalized: true}
	event := model.ChainEvent{ID: 4200000, BlockNum: 42, EventIdx: 0, ModuleId: "Balances", EventId: "Deposit", Params: model.EventParams{{Type: "AccountId", Value: account}}}
	tx, err := d.DbBegin()
	require.NoError(t, err)
	t.Cleanup(func() { d.DbRollback(tx) })
	require.NoError(t, d.CreateBlock(ctx, tx, &block))
	require.NoError(t, d.CreateEvent(tx, []model.ChainEvent{event}))
	tasks := service.BuildPluginProjections(42, []model.ChainEvent{event}, nil)
	require.NoError(t, d.CreatePluginProjections(tx, tasks))
	require.NoError(t, d.DbCommit(tx))
	// An auxiliary RPC failure is terminal only after its observation is durable.
	require.NoError(t, db.Exec(`CREATE FUNCTION reject_unknown_observation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected observation failure'; END $$`).Error)
	require.NoError(t, db.Exec(`CREATE TRIGGER reject_unknown_observation BEFORE INSERT ON balance_account_observations FOR EACH ROW EXECUTE FUNCTION reject_unknown_observation()`).Error)
	for _, task := range tasks {
		if task.Kind == "event" {
			require.ErrorContains(t, service.ProcessPluginProjection(ctx, d, task.ID), "observation failure")
			var row model.PluginProjection
			require.NoError(t, db.First(&row, "id = ?", task.ID).Error)
			require.Nil(t, row.CompletedAt)
		}
	}
	require.NoError(t, db.Exec("DROP TRIGGER reject_unknown_observation ON balance_account_observations").Error)
	for _, task := range tasks {
		require.NoError(t, service.ProcessPluginProjection(ctx, d, task.ID))
		var row model.PluginProjection
		require.NoError(t, db.First(&row, "id = ?", task.ID).Error)
		require.NotNil(t, row.CompletedAt)
	}
	var a bModel.Account
	require.NoError(t, db.First(&a, "address = ?", account).Error)
	require.Equal(t, "unknown", a.StateStatus)
	require.Nil(t, a.Balance)
	require.Contains(t, a.StateError, "pruned")
	require.Equal(t, block.Hash, a.StateHash)
	var observation bModel.AccountObservation
	require.NoError(t, db.First(&observation, "address = ?", account).Error)
	require.Equal(t, "unknown", observation.StateStatus)
}

func TestInitAccountHistoricalState(t *testing.T) {
	d, storageDAO, pool, conn := storageFixture(t)
	ctx := context.Background()
	db := storageDAO.GetDbInstance().(*gorm.DB)
	from, to := strings.Repeat("ab", 32), strings.Repeat("cd", 32)
	accountRPC(t, from, to)
	storageDAO.SetPrefix("balance")
	plugin := balance.New()
	plugin.InitDao(storageDAO)
	plugin.SetRedisPool(pool)
	tx, err := d.DbBegin()
	require.NoError(t, err)
	t.Cleanup(func() { d.DbRollback(tx) })
	require.NoError(t, d.CreateBlock(ctx, tx, &model.ChainBlock{ID: 42, BlockNum: 42, Hash: "0x" + strings.Repeat("42", 32), Finalized: true}))
	require.NoError(t, d.DbCommit(tx))
	_, err = conn.Do("HSET", model.MetadataCacheKey(), "finalized_blockNum", 42)
	require.NoError(t, err)
	require.NoError(t, bDao.InitAccount(&bDao.Storage{Dao: storageDAO, Pool: pool}))
	var accounts []bModel.Account
	require.NoError(t, db.Find(&accounts).Error)
	require.Len(t, accounts, 2)
	for _, a := range accounts {
		require.Equal(t, "available", a.StateStatus)
		require.Equal(t, "100", a.Balance.String())
		require.Equal(t, uint(42), *a.StateBlockNum)
	}
}
