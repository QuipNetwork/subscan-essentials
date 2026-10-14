package service

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/itering/scale.go/types"
	"github.com/itering/subscan/model"
	"github.com/itering/subscan/util/mq"
	substrate "github.com/itering/substrate-api-rpc"
	"github.com/itering/substrate-api-rpc/hasher"
	"github.com/itering/substrate-api-rpc/metadata"
	"github.com/stretchr/testify/require"
)

func sessionMetadata(prefix, modifier string) *metadata.Instant {
	u32, validators := "U32", "Vec<AccountId>"
	return &metadata.Instant{Metadata: types.MetadataTag{Modules: []types.MetadataModules{{Name: "Session", Prefix: prefix, Storage: []types.MetadataStorage{
		{Name: "CurrentIndex", Modifier: modifier, Fallback: "00000000", Type: types.StorageType{PlainType: &u32}},
		{Name: "Validators", Modifier: modifier, Fallback: "00", Type: types.StorageType{PlainType: &validators}},
	}}}}}
}

func babeAuthorDigest(index uint32) []byte {
	data := make([]byte, 13)
	data[0] = 2
	binary.LittleEndian.PutUint32(data[1:], index)
	raw, _ := json.Marshal(substrate.PreRuntime{Engine: substrate.CidBabe, Data: hex.EncodeToString(data)})
	return raw
}

func TestBlockStateAvailability(t *testing.T) {
	const spec = 900220
	isolateRuntimeSpecs(t, spec)
	m := sessionMetadata("HistoricalSession", "Default")
	metadata.RuntimeMetadata[spec] = m
	hash := "0x" + strings.Repeat("22", 32)
	account := strings.Repeat("ab", 32)
	key := func(item string) string {
		return "0x" + hex.EncodeToString(append(hasher.HashByCryptoName([]byte("HistoricalSession"), "Twox128"), hasher.HashByCryptoName([]byte(item), "Twox128")...))
	}
	var session interface{}
	var validators interface{} = "0x04" + account
	var runtimeErr, storageErr error
	var responseMu sync.RWMutex
	calls := executionRPC(t, func(method string, params []interface{}) (interface{}, error) {
		responseMu.RLock()
		defer responseMu.RUnlock()
		switch method {
		case "state_getRuntimeVersion":
			if len(params) == 1 && params[0] == hash {
				return map[string]interface{}{"specVersion": spec}, runtimeErr
			}
		case "state_getStorageAt":
			if len(params) != 2 || params[1] != hash {
				return nil, fmt.Errorf("wrong storage state")
			}
			if params[0] == key("CurrentIndex") {
				return session, storageErr
			}
			if params[0] == key("Validators") {
				return validators, storageErr
			}
		}
		return nil, fmt.Errorf("unexpected %s %v", method, params)
	})
	s := &Service{}
	for _, mode := range []string{"default zero", "observed zero", "rotation", "optional absent", "rpc unknown", "runtime unknown", "truncated index", "trailing index", "bad hex", "bad validators", "out of bounds", "missing digest"} {
		t.Run(mode, func(t *testing.T) {
			responseMu.Lock()
			session, validators, runtimeErr, storageErr = nil, "0x04"+account, nil, nil
			m.Metadata.Modules[0].Storage[0].Modifier = "Default"
			digest := babeAuthorDigest(0)
			switch mode {
			case "observed zero":
				session = "0x00000000"
			case "rotation":
				session = "0x01000000"
				validators = "0x08" + strings.Repeat("cd", 32) + account
				digest = babeAuthorDigest(1)
			case "optional absent":
				m.Metadata.Modules[0].Storage[0].Modifier = "Optional"
			case "rpc unknown":
				storageErr = fmt.Errorf("historical state pruned")
			case "runtime unknown":
				runtimeErr = fmt.Errorf("runtime unavailable")
			case "truncated index":
				session = "0x00"
			case "trailing index":
				session = "0x0000000000"
			case "bad hex":
				session = "0xzz"
			case "bad validators":
				validators = "0x04ab"
			case "out of bounds":
				digest = babeAuthorDigest(1)
			case "missing digest":
				digest = nil
			}
			responseMu.Unlock()
			block := &model.ChainBlock{Hash: hash, ParentHash: "parent-must-not-be-used", SpecVersion: spec - 1}
			s.enrichBlockState(context.Background(), block, digest)
			require.Equal(t, hash, block.SessionStateHash)
			require.Equal(t, hash, block.AuthorStateHash)
			switch mode {
			case "optional absent":
				require.Equal(t, model.StateAbsent, block.SessionStatus)
				require.Nil(t, block.SessionIndex)
			case "rpc unknown", "runtime unknown", "truncated index", "trailing index", "bad hex":
				require.Equal(t, model.StateUnknown, block.SessionStatus)
				require.Nil(t, block.SessionIndex)
				require.NotEmpty(t, block.SessionError)
			default:
				require.Equal(t, model.StateAvailable, block.SessionStatus, block.SessionError)
				require.NotNil(t, block.SessionIndex)
				if mode == "rotation" {
					require.EqualValues(t, 1, *block.SessionIndex)
				} else {
					require.Zero(t, *block.SessionIndex)
				}
			}
			encoded, err := json.Marshal(s.BlockAsSampleJson(block))
			require.NoError(t, err)
			var api map[string]interface{}
			require.NoError(t, json.Unmarshal(encoded, &api))
			if block.SessionIndex == nil {
				require.Nil(t, api["session_index"])
			} else {
				require.Equal(t, float64(*block.SessionIndex), api["session_index"])
			}
			require.Equal(t, block.AuthorStatus, api["author_status"])
			switch mode {
			case "rpc unknown", "runtime unknown", "bad validators", "out of bounds", "missing digest":
				require.Equal(t, model.StateUnknown, block.AuthorStatus)
				require.Empty(t, block.Validator)
				require.NotEmpty(t, block.AuthorError)
			default:
				require.Equal(t, model.StateAvailable, block.AuthorStatus, block.AuthorError)
				require.Equal(t, account, block.Validator)
			}
		})
	}
	// Unknown auxiliary state must still reach the actual core commit path.
	d := newExecutionDAO(t)
	block, events, execution := integrityInputs(t)
	const executionSpec = 900221
	isolateRuntimeSpecs(t, executionSpec)
	metadata.RuntimeMetadata[executionSpec] = execution
	previousQueue := mq.Instant
	mq.Instant = &blockIntegrityQueue{dao: &d.blockIntegrityDao}
	t.Cleanup(func() { mq.Instant = previousQueue })
	responseMu.Lock()
	runtimeErr = fmt.Errorf("unavailable auxiliary runtime")
	responseMu.Unlock()
	require.NoError(t, (&Service{dao: d}).CreateChainBlock(context.Background(), hash, block, events, executionSpec, 99))
	require.True(t, d.committed)
	require.Equal(t, model.StateUnknown, d.blocks[42].SessionStatus)
	require.Equal(t, model.StateUnknown, d.blocks[42].AuthorStatus)
	require.Nil(t, d.blocks[42].SessionIndex, "legacy numeric argument is not evidence")
	for _, call := range calls() {
		if call.Method == "state_getStorageAt" {
			require.Equal(t, hash, call.Params[1])
		}
	}
}

func TestAuthorDigestValidation(t *testing.T) {
	account := strings.Repeat("ab", 32)
	for _, data := range [][]byte{nil, []byte(`{}`), []byte(`{"engine":1161969986,"data":"02"}`), babeAuthorDigest(3)} {
		_, err := authorFromDigest(data, []string{account})
		require.Error(t, err)
	}
	for _, variant := range []byte{1, 2, 3} {
		size := 109
		if variant == 2 {
			size = 13
		}
		data := make([]byte, size)
		data[0] = variant
		raw, err := json.Marshal(substrate.PreRuntime{Engine: substrate.CidBabe, Data: hex.EncodeToString(data)})
		require.NoError(t, err)
		author, err := authorFromDigest(raw, []string{account})
		require.NoError(t, err)
		require.Equal(t, account, author)
	}
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, ^uint64(0))
	raw, err := json.Marshal(substrate.PreRuntime{Engine: substrate.CidAura, Data: hex.EncodeToString(data)})
	require.NoError(t, err)
	author, err := authorFromDigest(raw, []string{account})
	require.NoError(t, err)
	require.Equal(t, account, author)
}

// The null response here is synthetic. This proves metadata defaults, not
// historical auxiliary state availability (in particular local-119 is lost).
func TestArchivedSessionMetadataDefaults(t *testing.T) {
	root := os.Getenv("QUIP_CONFORMANCE_FIXTURES")
	if root == "" {
		t.Skip("requires reviewed decoder forks and QUIP_CONFORMANCE_FIXTURES")
	}
	isolateRuntimeSpecs(t, 117, 118, 119)
	hash := "0x" + strings.Repeat("33", 32)
	calls := executionRPC(t, func(method string, params []interface{}) (interface{}, error) {
		if method == "state_getStorageAt" && len(params) == 2 && params[1] == hash {
			return nil, nil
		}
		return nil, fmt.Errorf("unexpected %s %v", method, params)
	})
	for _, network := range []string{"aglais-117-118", "local-119"} {
		dir := filepath.Join(root, network)
		data, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
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
		require.NoError(t, json.Unmarshal(data, &manifest))
		for _, block := range manifest.Blocks {
			raw, err := os.ReadFile(filepath.Join(dir, block.MetadataFile))
			require.NoError(t, err)
			require.Equal(t, manifest.Files[block.MetadataFile].SHA256, fmt.Sprintf("%x", sha256.Sum256(raw)))
			m, err := registerCheckedMetadata(&metadata.RuntimeRaw{Spec: block.ExecutionSpecVersion, Raw: "0x" + hex.EncodeToString(raw)})
			require.NoError(t, err)
			value, status, err := readPlainState(context.Background(), hash, block.ExecutionSpecVersion, m, "Session", "CurrentIndex")
			require.NoError(t, err)
			require.Equal(t, model.StateAvailable, status)
			require.Equal(t, "0", string(value))
		}
	}
	require.Len(t, calls(), 5)
}
