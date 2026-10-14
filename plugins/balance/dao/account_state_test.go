package dao

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	ws "github.com/gorilla/websocket"
	"github.com/itering/scale.go/source"
	"github.com/itering/scale.go/types"
	"github.com/itering/subscan-plugin/storage"
	"github.com/itering/substrate-api-rpc/hasher"
	"github.com/itering/substrate-api-rpc/metadata"
	"github.com/itering/substrate-api-rpc/websocket"
	"github.com/stretchr/testify/require"
)

func TestAccountStateHistoricalAvailability(t *testing.T) {
	const spec = 900330
	types.RegCustomTypes(map[string]source.TypeStruct{
		"HistoricalAccount": {Type: "struct", TypeMapping: [][]string{{"nonce", "U32"}, {"consumers", "U32"}, {"providers", "U32"}, {"sufficients", "U32"}, {"data", "HistoricalBalance"}}},
		"HistoricalBalance": {Type: "struct", TypeMapping: [][]string{{"free", "U128"}, {"reserved", "U128"}, {"frozen", "U128"}, {"flags", "U128"}}},
	})
	m := &metadata.Instant{Metadata: types.MetadataTag{Modules: []types.MetadataModules{{Name: "System", Prefix: "HistoricalSystem", Storage: []types.MetadataStorage{{Name: "Account", Modifier: "Default", Fallback: strings.Repeat("00", 80), Type: types.StorageType{Origin: "MapType", MapType: &types.MapType{Hasher: "Blake2_128Concat", Key: "AccountId", Value: "HistoricalAccount"}}}}}}}}
	old := metadata.RuntimeMetadata[spec]
	metadata.RuntimeMetadata[spec] = m
	t.Cleanup(func() {
		if old == nil {
			delete(metadata.RuntimeMetadata, spec)
		} else {
			metadata.RuntimeMetadata[spec] = old
		}
	})
	hash := "0x" + strings.Repeat("32", 32)
	account := strings.Repeat("ab", 32)
	block := &storage.Block{BlockNum: 42, Hash: hash, Finalized: true, SpecVersion: spec - 1}
	keyBytes := append(hasher.HashByCryptoName([]byte("HistoricalSystem"), "Twox128"), hasher.HashByCryptoName([]byte("Account"), "Twox128")...)
	accountBytes, _ := hex.DecodeString(account)
	key := "0x" + hex.EncodeToString(append(keyBytes, hasher.HashByCryptoName(accountBytes, "Blake2_128Concat")...))
	// Each immutable responder rejects any latest, parent, or wrong-metadata read.
	run := func(t *testing.T, mode string) {
		raw := make([]byte, 80)
		binary.LittleEndian.PutUint32(raw, 7)
		binary.LittleEndian.PutUint64(raw[16:], 9007199254740993)
		binary.LittleEndian.PutUint64(raw[32:], 2)
		binary.LittleEndian.PutUint64(raw[48:], 9)
		var result interface{} = "0x" + hex.EncodeToString(raw)
		switch mode {
		case "default null", "optional null", "bad fallback":
			result = nil
		case "zero":
			result = "0x" + strings.Repeat("00", 80)
		case "truncated":
			result = "0x" + hex.EncodeToString(raw[:79])
		case "trailing":
			result = "0x" + hex.EncodeToString(append(raw, 0))
		case "bad hex":
			result = "0xqq"
		case "wrong type":
			result = map[string]interface{}{}
		}
		calls := 0
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
				if conn.ReadJSON(&req) != nil {
					return
				}
				calls++
				response := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
				var failure string
				switch {
				case req.Method == "state_getRuntimeVersion" && len(req.Params) == 1 && req.Params[0] == hash:
					response["result"] = map[string]interface{}{"specVersion": spec}
					if mode == "runtime failure" {
						failure = "pruned runtime"
					}
					if mode == "missing runtime" {
						response["result"] = nil
					}
				case req.Method == "state_getStorageAt" && len(req.Params) == 2 && req.Params[0] == key && req.Params[1] == hash:
					response["result"] = result
					if mode == "RPC failure" {
						failure = "pruned account"
					}
					if mode == "missing result" {
						delete(response, "result")
					}
				default:
					failure = fmt.Sprintf("unexpected request %s %v", req.Method, req.Params)
					t.Error(failure)
				}
				if failure != "" {
					delete(response, "result")
					response["error"] = map[string]interface{}{"code": -32000, "message": failure}
				}
				if conn.WriteJSON(response) != nil {
					return
				}
			}
		}))
		websocket.Close()
		websocket.SetEndpoint("ws" + strings.TrimPrefix(server.URL, "http"))
		out := ReadAccountState(context.Background(), block, account)
		websocket.Close()
		server.Close()
		require.Equal(t, hash, out.StateHash)
		require.Equal(t, uint(42), *out.StateBlockNum)
		switch mode {
		case "valid":
			require.Equal(t, "available", out.StateStatus)
			require.Equal(t, uint32(7), *out.Nonce)
			require.Equal(t, "9007199254740995", out.Balance.String())
			require.Equal(t, "9", out.Locked.String())
			require.Equal(t, "2", out.Reserved.String())
			require.Equal(t, spec, *out.StateSpecVersion)
		case "default null", "zero":
			require.Equal(t, "available", out.StateStatus)
			require.Zero(t, *out.Nonce)
			require.Equal(t, "0", out.Balance.String())
		case "optional null":
			require.Equal(t, "absent", out.StateStatus)
			require.Nil(t, out.Nonce)
			require.Nil(t, out.Balance)
			require.Empty(t, out.StateError)
		default:
			require.Equal(t, "unknown", out.StateStatus)
			require.NotEmpty(t, out.StateError)
			require.Nil(t, out.Nonce)
			require.Nil(t, out.Balance)
		}
		require.Greater(t, calls, 0)
	}
	for _, mode := range []string{"valid", "default null", "optional null", "zero", "truncated", "trailing", "bad hex", "wrong type", "runtime failure", "missing runtime", "RPC failure", "missing result", "bad schema", "bad hasher", "bad fallback"} {
		t.Run(mode, func(t *testing.T) {
			field := &m.Metadata.Modules[0].Storage[0]
			field.Modifier = "Default"
			field.Fallback = strings.Repeat("00", 80)
			field.Type.MapType.Hasher = "Blake2_128Concat"
			field.Type.MapType.Value = "HistoricalAccount"
			switch mode {
			case "optional null":
				field.Modifier = "Optional"
			case "bad schema":
				field.Type.MapType.Value = "U32"
			case "bad hasher":
				field.Type.MapType.Hasher = "unrecognized"
			case "bad fallback":
				field.Fallback = "00"
			}
			run(t, mode)
		})
	}
	for _, b := range []*storage.Block{nil, {Hash: "", Finalized: true}, {Hash: hash, Finalized: false}, {Hash: hash, Finalized: true, BlockNum: -1}} {
		out := ReadAccountState(context.Background(), b, account)
		require.Equal(t, "unknown", out.StateStatus)
		require.NotEmpty(t, out.StateError)
	}
}

func TestAccountDataStrictFields(t *testing.T) {
	for _, raw := range []string{`{}`, `{"nonce":0,"data":{"free":"0","reserved":"0"}}`, `{"nonce":4294967296}`, `{"nonce":-1}`, `{"nonce":0,"data":{"free":"1e2","reserved":"0","frozen":"0"}}`, `{"nonce":0,"data":{"free":"340282366920938463463374607431768211456","reserved":"0","frozen":"0"}}`} {
		_, err := parseAccountData([]byte(raw))
		require.Error(t, err, raw)
	}
	for _, field := range []string{`"miscFrozen":"4","feeFrozen":"6"`, `"misc_frozen":"4","fee_frozen":"6"`, `"frozen":"6"`} {
		out, err := parseAccountData([]byte(`{"nonce":0,"data":{"free":"0","reserved":"0",` + field + `}}`))
		require.NoError(t, err)
		require.Equal(t, "6", out.Locked.String())
	}
}
