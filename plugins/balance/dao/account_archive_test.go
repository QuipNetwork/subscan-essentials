package dao

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	ws "github.com/gorilla/websocket"
	"github.com/itering/subscan-plugin/storage"
	"github.com/itering/subscan/share/state"
	"github.com/itering/substrate-api-rpc/hasher"
	"github.com/itering/substrate-api-rpc/metadata"
	"github.com/itering/substrate-api-rpc/websocket"
	"github.com/stretchr/testify/require"
)

// Metadata is chain-captured and SHA-verified. RPC null is synthetic: this proves
// each supported account schema/default, not availability of historical accounts.
func TestArchivedAccountMetadataDefaults(t *testing.T) {
	root := os.Getenv("QUIP_CONFORMANCE_FIXTURES")
	if root == "" {
		t.Skip("requires reviewed decoder forks and QUIP_CONFORMANCE_FIXTURES")
	}
	saved := map[int]*metadata.Instant{}
	for _, spec := range []int{117, 118, 119} {
		saved[spec] = metadata.RuntimeMetadata[spec]
	}
	t.Cleanup(func() {
		for spec, m := range saved {
			if m == nil {
				delete(metadata.RuntimeMetadata, spec)
			} else {
				metadata.RuntimeMetadata[spec] = m
			}
		}
	})
	var mu sync.RWMutex
	var spec int
	var key string
	hash := "0x" + strings.Repeat("33", 32)
	account := strings.Repeat("ab", 32)
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
			response := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
			mu.RLock()
			switch {
			case req.Method == "state_getRuntimeVersion" && len(req.Params) == 1 && req.Params[0] == hash:
				response["result"] = map[string]interface{}{"specVersion": spec}
			case req.Method == "state_getStorageAt" && len(req.Params) == 2 && req.Params[0] == key && req.Params[1] == hash:
				response["result"] = nil
			default:
				t.Errorf("unexpected RPC %s %v", req.Method, req.Params)
				response["error"] = map[string]interface{}{"code": -32601, "message": "unexpected request"}
			}
			mu.RUnlock()
			if conn.WriteJSON(response) != nil {
				return
			}
		}
	}))
	websocket.Close()
	websocket.SetEndpoint("ws" + strings.TrimPrefix(server.URL, "http"))
	t.Cleanup(func() { websocket.Close(); server.Close() })
	count := 0
	for _, network := range []string{"aglais-117-118", "local-119"} {
		dir := filepath.Join(root, network)
		raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
		require.NoError(t, err)
		var manifest struct {
			Blocks []struct {
				MetadataFile         string `json:"metadataFile"`
				ExecutionSpecVersion int    `json:"executionSpecVersion"`
			} `json:"blocks"`
			Files map[string]struct {
				SHA256 string `json:"sha256"`
			} `json:"files"`
		}
		require.NoError(t, json.Unmarshal(raw, &manifest))
		for _, block := range manifest.Blocks {
			raw, err := os.ReadFile(filepath.Join(dir, block.MetadataFile))
			require.NoError(t, err)
			require.Equal(t, manifest.Files[block.MetadataFile].SHA256, fmt.Sprintf("%x", sha256.Sum256(raw)))
			m, err := state.RegisterCheckedMetadata(&metadata.RuntimeRaw{Spec: block.ExecutionSpecVersion, Raw: "0x" + hex.EncodeToString(raw)})
			require.NoError(t, err)
			_, prefix, _, hashName, err := accountStorage(m, block.ExecutionSpecVersion)
			require.NoError(t, err)
			accountBytes, _ := hex.DecodeString(account)
			mu.Lock()
			spec = block.ExecutionSpecVersion
			key = "0x" + hex.EncodeToString(append(prefix, hasher.HashByCryptoName(accountBytes, hashName)...))
			mu.Unlock()
			out := ReadAccountState(context.Background(), &storage.Block{Hash: hash, BlockNum: 42, SpecVersion: spec - 1, Finalized: true}, account)
			require.Equal(t, "available", out.StateStatus, out.StateError)
			require.Equal(t, spec, *out.StateSpecVersion)
			require.Zero(t, *out.Nonce)
			require.Equal(t, "0", out.Balance.String())
			require.Equal(t, "0", out.Locked.String())
			count++
		}
	}
	require.Equal(t, 5, count)
}
