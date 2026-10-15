package evm_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ws "github.com/gorilla/websocket"
	"github.com/itering/scale.go/types"
	"github.com/itering/subscan-plugin/storage"
	"github.com/itering/subscan/plugins"
	"github.com/itering/subscan/share/web3"
	"github.com/itering/substrate-api-rpc/metadata"
	"github.com/stretchr/testify/require"
)

type startupDAO struct {
	storage.Dao
	migrations int
}

func (d *startupDAO) AutoMigration(interface{}) error { d.migrations++; return nil }

// Run a fresh process so the recorder observes package initializers, before any
// test could replace providers or hide an import-time eth_chainId request.
func TestEVMStartupRPCGate(t *testing.T) {
	if mode := os.Getenv("QUIP_EVM_STARTUP_CHILD"); mode != "" {
		plugin := plugins.RegisteredPlugins["evm"]
		require.NotNil(t, plugin)
		require.False(t, plugin.Enable())
		require.Zero(t, web3.CHAIN_ID)
		if mode == "registered only" {
			return
		}
		modules := []types.MetadataModules{{Name: "System"}, {Name: "Balances"}}
		if mode == "EVM" || mode == "Revive" || mode == "RPC failure" {
			module := mode
			if module == "RPC failure" {
				module = "EVM"
			}
			modules = append(modules, types.MetadataModules{Name: module})
		}
		m := metadata.RegNewMetadataType(900400, "0x6d6574610d000400")
		m.Metadata.Modules = modules
		d := &startupDAO{}
		plugin.InitDao(d)
		if mode == "unsupported runtime" {
			require.False(t, plugin.Enable())
			require.Zero(t, d.migrations)
			require.Zero(t, web3.CHAIN_ID)
		} else {
			require.True(t, plugin.Enable())
			require.Positive(t, d.migrations)
			if mode == "RPC failure" {
				require.Zero(t, web3.CHAIN_ID)
			} else {
				require.Equal(t, int64(42), web3.CHAIN_ID)
			}
			web3.InitChainID(context.Background()) // repeat activation must not probe twice
		}
		return
	}
	for _, transport := range []string{"http", "websocket"} {
		for _, mode := range []string{"registered only", "unsupported runtime", "EVM", "Revive", "RPC failure"} {
			t.Run(transport+"/"+mode, func(t *testing.T) {
				var connections, calls, unexpected atomic.Int64
				handle := func(raw []byte) []byte {
					calls.Add(1)
					var req struct {
						ID     interface{} `json:"id"`
						Method string      `json:"method"`
					}
					if json.Unmarshal(raw, &req) != nil || req.Method != "eth_chainId" {
						unexpected.Add(1)
					}
					response := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID, "result": "0x2a"}
					if mode == "RPC failure" {
						delete(response, "result")
						response["error"] = map[string]interface{}{"code": -32601, "message": "eth method unavailable"}
					}
					data, _ := json.Marshal(response)
					return data
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					connections.Add(1)
					if ws.IsWebSocketUpgrade(r) {
						conn, err := (&ws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
						if err != nil {
							return
						}
						defer conn.Close()
						_, raw, err := conn.ReadMessage()
						if err != nil {
							return
						}
						_ = conn.WriteMessage(ws.TextMessage, handle(raw))
						return
					}
					var raw json.RawMessage
					if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
						unexpected.Add(1)
						return
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write(handle(raw))
				}))
				defer server.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestEVMStartupRPCGate$", "-test.count=1")
				for _, env := range os.Environ() {
					if !strings.HasPrefix(env, "ETH_RPC=") && !strings.HasPrefix(env, "CHAIN_WS_ENDPOINT=") && !strings.HasPrefix(env, "QUIP_EVM_STARTUP_CHILD=") {
						cmd.Env = append(cmd.Env, env)
					}
				}
				eth := ""
				if transport == "http" {
					eth = server.URL
				}
				cmd.Env = append(cmd.Env, "QUIP_EVM_STARTUP_CHILD="+mode, "ETH_RPC="+eth, "CHAIN_WS_ENDPOINT=ws"+strings.TrimPrefix(server.URL, "http"))
				output, err := cmd.CombinedOutput()
				require.NoError(t, err, string(output))
				require.Zero(t, unexpected.Load())
				if mode == "registered only" || mode == "unsupported runtime" {
					require.Zero(t, connections.Load(), "disabled/import-only startup must make no connection")
					require.Zero(t, calls.Load())
				} else {
					require.Equal(t, int64(1), connections.Load())
					require.Equal(t, int64(1), calls.Load())
				}
			})
		}
	}
}
