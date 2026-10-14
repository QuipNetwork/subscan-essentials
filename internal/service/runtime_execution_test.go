package service

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
	"github.com/itering/subscan/model"
	"github.com/itering/subscan/plugins"
	"github.com/itering/subscan/util"
	substrate "github.com/itering/substrate-api-rpc"
	"github.com/itering/substrate-api-rpc/metadata"
	rpcmodel "github.com/itering/substrate-api-rpc/model"
	"github.com/itering/substrate-api-rpc/websocket"
	"github.com/stretchr/testify/require"
)

type executionDAO struct {
	transactionFaultDao
	raw        map[int]*metadata.RuntimeRaw
	registered map[int]uint
}

func (d *executionDAO) RuntimeVersionRaw(spec int) *metadata.RuntimeRaw { return d.raw[spec] }
func (d *executionDAO) CreateRuntimeVersion(_ context.Context, _ string, spec int, height uint) bool {
	d.registered[spec] = height
	return d.raw[spec] == nil
}
func (d *executionDAO) SetRuntimeData(spec int, _, raw string) int64 {
	d.raw[spec] = &metadata.RuntimeRaw{Spec: spec, Raw: raw}
	return 1
}
func newExecutionDAO(t *testing.T) *executionDAO {
	t.Helper()
	oldPlugins, oldSpec := plugins.RegisteredPlugins, util.CurrentRuntimeSpecVersion
	plugins.RegisteredPlugins = map[string]plugins.PluginFactory{}
	t.Cleanup(func() { plugins.RegisteredPlugins = oldPlugins; util.CurrentRuntimeSpecVersion = oldSpec })
	return &executionDAO{transactionFaultDao: transactionFaultDao{blockIntegrityDao: blockIntegrityDao{
		blocks: map[uint]model.ChainBlock{}, events: map[uint]model.ChainEvent{}, extrinsics: map[uint]model.ChainExtrinsic{},
	}}, raw: map[int]*metadata.RuntimeRaw{}, registered: map[int]uint{}}
}
func isolateRuntimeSpecs(t *testing.T, specs ...int) {
	for _, spec := range specs {
		old := metadata.RuntimeMetadata[spec]
		delete(metadata.RuntimeMetadata, spec)
		t.Cleanup(func() {
			if old == nil {
				delete(metadata.RuntimeMetadata, spec)
			} else {
				metadata.RuntimeMetadata[spec] = old
			}
		})
	}
}

// Explicit synthetic V13 metadata: one call taking U32/U64 and one event.
func executionMetadata(argType, eventName string) string {
	data := append([]byte("meta"), 13, 4)
	str := func(s string) { data = append(data, byte(len(s)<<2)); data = append(data, []byte(s)...) }
	str("Example")
	data = append(data, 0, 1, 4) // no storage; one call
	str("set_value")
	data = append(data, 4)
	str("value")
	str(argType)
	data = append(data, 0)
	data = append(data, 1, 4)
	str(eventName)
	data = append(data, 0, 0) // event, no args/docs
	moduleIndex := byte(0)
	if argType == "U64" {
		moduleIndex = 1
	} // upgrade moves the call/event index as well as changing the argument
	data = append(data, 0, 0, moduleIndex, 4, 0) // constants, errors, module index, ext version/extensions
	return "0x" + hex.EncodeToString(data)
}

type executionRequest struct {
	Method string
	Params []interface{}
}

func executionRPC(t *testing.T, respond func(string, []interface{}) (interface{}, error)) func() []executionRequest {
	t.Helper()
	var mu sync.Mutex
	var requests []executionRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := (&ws.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			var req struct {
				ID     int           `json:"id"`
				Method string        `json:"method"`
				Params []interface{} `json:"params"`
			}
			if c.ReadJSON(&req) != nil {
				return
			}
			mu.Lock()
			requests = append(requests, executionRequest{req.Method, req.Params})
			mu.Unlock()
			result, err := respond(req.Method, req.Params)
			response := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID, "result": result}
			if err != nil {
				delete(response, "result")
				response["error"] = map[string]interface{}{"code": -32000, "message": err.Error()}
			}
			if c.WriteJSON(response) != nil {
				return
			}
		}
	}))
	websocket.Close()
	websocket.SetEndpoint("ws" + strings.TrimPrefix(server.URL, "http"))
	t.Cleanup(func() { websocket.Close(); server.Close() })
	return func() []executionRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]executionRequest(nil), requests...)
	}
}

func TestFillUsesExecutionRuntime(t *testing.T) {
	websocket.SetChannelPoolMaxCap(1)
	t.Cleanup(func() { websocket.SetChannelPoolMaxCap(25) })
	const parentSpec, postSpec = 900201, 900202
	for _, mode := range []string{"upgrade", "ordinary", "genesis", "missing parent", "wrong header", "runtime error", "runtime null", "runtime malformed", "metadata error", "metadata malformed"} {
		t.Run(mode, func(t *testing.T) {
			isolateRuntimeSpecs(t, parentSpec, postSpec)
			d := newExecutionDAO(t)
			s := &Service{dao: d}
			block, events, _ := integrityInputs(t)
			hash := "0x" + strings.Repeat("2", 64)
			parent := block.Header.ParentHash
			num := uint(42)
			if mode == "genesis" {
				num = 0
				block.Header.Number = "0x0"
				block.Header.ParentHash = "0x" + strings.Repeat("0", 64)
				block.Extrinsics = nil
				events = "0x00"
				parent = hash
			}
			if mode == "missing parent" {
				block.Header.ParentHash = ""
			}
			if mode == "wrong header" {
				block.Header.Number = "0x2b"
			}
			calls := executionRPC(t, func(method string, params []interface{}) (interface{}, error) {
				switch method {
				case "chain_getBlockHash":
					return hash, nil
				case "chain_getBlock":
					return rpcmodel.BlockResult{Block: *block}, nil
				case "state_getStorageAt":
					if len(params) != 2 || params[1] != hash {
						return nil, fmt.Errorf("wrong storage state")
					}
					if params[0] == util.AddHex(util.EventStorageKey) {
						if mode == "genesis" {
							return nil, nil
						}
						return events, nil
					}
					if params[0] == util.AddHex(util.SessionIndexStorageKey) {
						return "0x00000000", nil
					}
				case "state_getRuntimeVersion":
					if len(params) != 1 {
						return nil, fmt.Errorf("missing runtime state")
					}
					if params[0] == parent {
						switch mode {
						case "runtime error":
							return nil, fmt.Errorf("unavailable parent runtime")
						case "runtime null":
							return nil, nil
						case "runtime malformed":
							return "bad", nil
						}
						return map[string]interface{}{"specVersion": parentSpec, "implName": "synthetic"}, nil
					}
					if params[0] == hash {
						if mode == "ordinary" {
							return map[string]interface{}{"specVersion": parentSpec, "implName": "synthetic"}, nil
						}
						return map[string]interface{}{"specVersion": postSpec, "implName": "synthetic"}, nil
					}
				case "state_getMetadata":
					if len(params) != 1 {
						return nil, fmt.Errorf("missing metadata state")
					}
					if params[0] == parent {
						if mode == "metadata error" {
							return nil, fmt.Errorf("unavailable parent metadata")
						}
						if mode == "metadata malformed" {
							return "0x00", nil
						}
						return executionMetadata("U32", "Ready"), nil
					}
					if params[0] == hash {
						return executionMetadata("U64", "Changed"), nil
					}
				}
				return nil, fmt.Errorf("unexpected request %s %v", method, params)
			})
			// Even a known latest runtime must not rescue missing parent information.
			util.CurrentRuntimeSpecVersion = postSpec
			err := s.FillBlockData(context.Background(), num, false)
			if mode != "upgrade" && mode != "ordinary" && mode != "genesis" {
				require.Error(t, err)
				switch mode {
				case "missing parent":
					require.ErrorContains(t, err, "invalid execution parent")
				case "wrong header":
					require.ErrorContains(t, err, "received header")
				case "runtime error", "runtime null", "runtime malformed":
					require.ErrorContains(t, err, "execution runtime")
				case "metadata error", "metadata malformed":
					require.ErrorContains(t, err, "execution metadata")
				}
				require.Empty(t, d.actions)
				require.False(t, d.committed)
				return
			}
			require.NoError(t, err)
			require.Equal(t, parentSpec, d.blocks[num].SpecVersion)
			require.True(t, d.committed)
			if mode == "upgrade" {
				require.Equal(t, uint(41), d.registered[parentSpec])
				require.Equal(t, uint(42), d.registered[postSpec])
				require.Len(t, d.extrinsics, 3)
				require.Len(t, d.events, 3)
				for _, event := range d.events {
					require.Equal(t, "Ready", event.EventId)
				}
				wrong, wrongErr := substrate.DecodeExtrinsic(block.Extrinsics, metadata.RuntimeMetadata[postSpec], postSpec)
				if wrongErr == nil {
					decodedEvents, eventErr := substrate.DecodeEvent(events, metadata.RuntimeMetadata[parentSpec], parentSpec)
					require.NoError(t, eventErr)
					_, _, wrongErr = convertBlockData(block.Extrinsics, events, wrong, decodedEvents)
				}
				require.Error(t, wrongErr, "changed call must reject this body under post-state metadata")
			} else if mode == "ordinary" {
				require.Equal(t, map[int]uint{parentSpec: 41}, d.registered)
			} else {
				require.Equal(t, map[int]uint{parentSpec: 0}, d.registered)
			}
			var runtimeHashes, metadataHashes []string
			for _, call := range calls() {
				if call.Method == "state_getRuntimeVersion" {
					runtimeHashes = append(runtimeHashes, call.Params[0].(string))
				}
				if call.Method == "state_getMetadata" {
					metadataHashes = append(metadataHashes, call.Params[0].(string))
				}
			}
			if mode == "upgrade" {
				require.Equal(t, []string{parent, hash, hash}, runtimeHashes)
				require.ElementsMatch(t, []string{parent, hash}, metadataHashes)
			} else if mode == "ordinary" {
				require.Equal(t, []string{parent, hash, hash}, runtimeHashes)
				require.Equal(t, []string{parent}, metadataHashes)
			} else {
				require.Equal(t, []string{hash, hash}, runtimeHashes)
				require.Equal(t, []string{hash}, metadataHashes)
			}
		})
	}
}

func TestCreateBlockMetadataCacheMissUsesParent(t *testing.T) {
	const spec = 900203
	isolateRuntimeSpecs(t, spec)
	d := newExecutionDAO(t)
	block, events, _ := integrityInputs(t)
	calls := executionRPC(t, func(method string, params []interface{}) (interface{}, error) {
		if method == "state_getMetadata" && len(params) == 1 && params[0] == block.Header.ParentHash {
			return executionMetadata("U32", "Ready"), nil
		}
		return nil, fmt.Errorf("unexpected metadata request %s %v", method, params)
	})
	require.NoError(t, (&Service{dao: d}).CreateChainBlock(context.Background(), "block-hash", block, events, spec, 0))
	require.Len(t, calls(), 1)
	require.Equal(t, block.Header.ParentHash, calls()[0].Params[0])
	require.True(t, d.committed)
}

// Opt-in shared, byte-exact archive fixture. This checks the actual loader and
// body/event decoder; DB/API acceptance of the full image is a separate gate.
func TestArchivedUpgradeExecutionRuntime(t *testing.T) {
	root := os.Getenv("QUIP_CONFORMANCE_FIXTURES")
	if root == "" {
		t.Skip("set QUIP_CONFORMANCE_FIXTURES to the reviewed scale.go/testdata/quip-metadata-conformance directory")
	}
	root = filepath.Join(root, "aglais-117-118")
	var manifest struct {
		Blocks []struct {
			Number uint   `json:"number"`
			Hash   string `json:"hash"`
		} `json:"blocks"`
		Files map[string]struct {
			SHA256 string `json:"sha256"`
		} `json:"files"`
	}
	manifestBytes, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	require.NoError(t, err)
	require.Equal(t, "a77d59562ecab6aaf80173e70a1f6dcf210815f8af471c7ce356e7ef08bd4b89", fmt.Sprintf("%x", sha256.Sum256(manifestBytes)))
	require.NoError(t, json.Unmarshal(manifestBytes, &manifest))
	read := func(name string) []byte {
		data, err := os.ReadFile(filepath.Join(root, name))
		require.NoError(t, err)
		require.Equal(t, manifest.Files[name].SHA256, fmt.Sprintf("%x", sha256.Sum256(data)), name)
		return data
	}
	var block rpcmodel.BlockResult
	require.NoError(t, json.Unmarshal(read("block-330109.json"), &block))
	// The fixture's header is sufficient to select the exact execution state.
	parent := block.Block.Header.ParentHash
	hash := manifest.Blocks[0].Hash
	require.Equal(t, uint(330109), manifest.Blocks[0].Number)
	var child rpcmodel.BlockResult
	require.NoError(t, json.Unmarshal(read("block-330110.json"), &child))
	require.Equal(t, hash, child.Block.Header.ParentHash)
	var before, after map[string]interface{}
	require.NoError(t, json.Unmarshal(read("runtime-parent-330109.json"), &before))
	require.NoError(t, json.Unmarshal(read("runtime-post-330109.json"), &after))
	require.Equal(t, float64(117), before["specVersion"])
	require.Equal(t, float64(118), after["specVersion"])
	isolateRuntimeSpecs(t, 117, 118)
	d := newExecutionDAO(t)
	// Adapt captured V16 bytes to the loader's metadata request. This does not
	// test the eventual image's metadata-version negotiation with a live node.
	parentMetadata := "0x" + hex.EncodeToString(read("metadata-330109.scale"))
	postMetadata := "0x" + hex.EncodeToString(read("metadata-330110.scale"))
	calls := executionRPC(t, func(method string, params []interface{}) (interface{}, error) {
		if len(params) == 1 {
			switch method {
			case "state_getRuntimeVersion":
				if params[0] == parent {
					return before, nil
				}
				if params[0] == hash {
					return after, nil
				}
			case "state_getMetadata":
				if params[0] == parent {
					return parentMetadata, nil
				}
				// 330110's parent is 330109; its execution metadata is the post-upgrade state.
				if params[0] == hash {
					return postMetadata, nil
				}
			}
		}
		return nil, fmt.Errorf("unexpected fixture request %s %v", method, params)
	})
	executionHash, num, err := executionStateHash(hash, &block.Block)
	require.NoError(t, err)
	require.Equal(t, uint(330109), num)
	execution, err := (&Service{dao: d}).registerBlockRuntimes(context.Background(), nil, num, hash, executionHash)
	require.NoError(t, err)
	require.Equal(t, 117, execution.SpecVersion)
	m, err := (&Service{dao: d}).getMetadataInstant(execution.SpecVersion, executionHash)
	require.NoError(t, err)
	extrinsics, err := substrate.DecodeExtrinsic(block.Block.Extrinsics, m, execution.SpecVersion)
	require.NoError(t, err)
	require.Len(t, extrinsics, 8)
	encodedEvents := "0x" + hex.EncodeToString(read("events-330109.scale"))
	events, err := substrate.DecodeEvent(encodedEvents, m, execution.SpecVersion)
	require.NoError(t, err)
	_, converted, err := convertBlockData(block.Block.Extrinsics, encodedEvents, extrinsics, events)
	require.NoError(t, err)
	require.Len(t, converted, 36)
	require.Equal(t, uint(330108), d.registered[117])
	require.Equal(t, uint(330109), d.registered[118])
	require.Len(t, calls(), 4)
}

func TestRejectInvalidRuntimeMetadata(t *testing.T) {
	const spec = 900204
	for _, raw := range []string{"", "0xgg", "0x00", executionMetadata("U32", "Ready") + "00"} {
		isolateRuntimeSpecs(t, spec)
		instant, err := registerCheckedMetadata(&metadata.RuntimeRaw{Spec: spec, Raw: raw})
		require.Error(t, err)
		require.Nil(t, instant)
		require.Nil(t, metadata.RuntimeMetadata[spec])
	}
}
