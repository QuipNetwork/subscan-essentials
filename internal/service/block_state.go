package service

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"github.com/itering/scale.go/types"
	"github.com/itering/subscan/model"
	"github.com/itering/subscan/util/address"
	substrate "github.com/itering/substrate-api-rpc"
	"github.com/itering/substrate-api-rpc/hasher"
	"github.com/itering/substrate-api-rpc/metadata"
	rpcmodel "github.com/itering/substrate-api-rpc/model"
	"github.com/itering/substrate-api-rpc/rpc"
	"github.com/itering/substrate-api-rpc/storage"
	"github.com/itering/substrate-api-rpc/websocket"
)

// readPlainState binds both the storage key and SCALE type/default to the
// metadata of the requested state. Optional null is absent; Default null is
// decoded from metadata. Transport/RPC/schema/decode failures are unknown.
func readPlainState(ctx context.Context, hash string, spec int, m *metadata.Instant, pallet, item string) (storage.StateStorage, string, error) {
	unknown := func(err error) (storage.StateStorage, string, error) { return "", model.StateUnknown, err }
	if err := ctx.Err(); err != nil {
		return unknown(err)
	}
	if hash == "" || m == nil {
		return unknown(fmt.Errorf("explicit state and metadata required"))
	}
	var entry *types.MetadataStorage
	var prefix string
	for _, module := range m.Metadata.Modules {
		if module.Name != pallet {
			continue
		}
		for _, field := range module.Storage {
			if field.Name == item {
				copy := field
				entry = &copy
				prefix = module.Prefix
				break
			}
		}
	}
	if entry == nil || entry.Type.PlainType == nil || prefix == "" {
		return unknown(fmt.Errorf("missing plain storage %s.%s", pallet, item))
	}
	key := append(hasher.HashByCryptoName([]byte(prefix), "Twox128"), hasher.HashByCryptoName([]byte(item), "Twox128")...)
	var response struct {
		Result json.RawMessage `json:"result"`
		Error  *rpcmodel.Error `json:"error"`
	}
	if err := websocket.SendWsRequest(nil, &response, rpc.StateGetStorage(rand.Intn(10000), "0x"+hex.EncodeToString(key), hash)); err != nil {
		return unknown(err)
	}
	if response.Error != nil {
		return unknown(fmt.Errorf("storage RPC %d: %s", response.Error.Code, response.Error.Message))
	}
	if len(response.Result) == 0 {
		return unknown(fmt.Errorf("missing storage result"))
	}
	var raw string
	if bytes.Equal(bytes.TrimSpace(response.Result), []byte("null")) {
		switch entry.Modifier {
		case "Optional":
			return "", model.StateAbsent, nil
		case "Default":
			raw = entry.Fallback
		default:
			return unknown(fmt.Errorf("unknown storage modifier %q", entry.Modifier))
		}
	} else if err := json.Unmarshal(response.Result, &raw); err != nil {
		return unknown(err)
	}
	data, err := hex.DecodeString(strings.TrimPrefix(raw, "0x"))
	if err != nil || len(data) == 0 {
		return unknown(fmt.Errorf("invalid storage bytes for %s.%s", pallet, item))
	}
	// The legacy SCALE U32 decoder pads short input with zeroes.
	if pallet == "Session" && item == "CurrentIndex" && len(data) != 4 {
		return unknown(fmt.Errorf("session index must contain exactly four bytes"))
	}
	if pallet == "Session" && item == "Validators" {
		if err := validateAccountVector(data); err != nil {
			return unknown(err)
		}
	}
	value, consumed, err := storage.Decode(raw, *entry.Type.PlainType, &types.ScaleDecoderOption{Spec: spec, Metadata: (*types.MetadataStruct)(m)})
	if err != nil {
		return unknown(err)
	}
	if consumed != len(data) {
		return unknown(fmt.Errorf("storage consumed %d of %d bytes", consumed, len(data)))
	}
	return value, model.StateAvailable, nil
}

// Quip Session.Validators is Vec<AccountId32>. Check its envelope before the
// legacy decoder, which pads truncated AccountIds. Reject non-canonical counts.
func validateAccountVector(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("missing validator vector")
	}
	var count uint64
	prefix := 1
	switch data[0] & 3 {
	case 0:
		count = uint64(data[0] >> 2)
	case 1:
		if len(data) < 2 {
			return fmt.Errorf("short validator count")
		}
		prefix = 2
		count = uint64(binary.LittleEndian.Uint16(data) >> 2)
		if count < 64 {
			return fmt.Errorf("non-canonical validator count")
		}
	case 2:
		if len(data) < 4 {
			return fmt.Errorf("short validator count")
		}
		prefix = 4
		count = uint64(binary.LittleEndian.Uint32(data) >> 2)
		if count < 16384 {
			return fmt.Errorf("non-canonical validator count")
		}
	case 3:
		if data[0] != 3 || len(data) < 5 {
			return fmt.Errorf("invalid validator count")
		}
		prefix = 5
		count = uint64(binary.LittleEndian.Uint32(data[1:]))
		if count < 1<<30 {
			return fmt.Errorf("non-canonical validator count")
		}
	}
	if uint64(len(data)-prefix) != count*32 {
		return fmt.Errorf("validator count does not match AccountId32 bytes")
	}
	return nil
}

// Session observations describe the block's post-state. In particular, a
// NewSession block uses its new validator ordering, not the previous session's
// cache. The block body/events still decode with execution (parent) metadata.
func (s *Service) enrichBlockState(ctx context.Context, block *model.ChainBlock, preRuntime []byte) {
	block.BlockStateInfo = model.BlockStateInfo{SessionStatus: model.StateUnknown, AuthorStatus: model.StateUnknown, SessionStateHash: block.Hash, AuthorStateHash: block.Hash}
	block.Validator = ""
	state, hashErr := hex.DecodeString(strings.TrimPrefix(block.Hash, "0x"))
	if hashErr != nil || len(state) != 32 || bytes.Equal(state, make([]byte, 32)) {
		block.SessionError = "invalid explicit block state hash"
		block.AuthorError = block.SessionError
		return
	}
	if err := ctx.Err(); err != nil {
		block.SessionError = err.Error()
		block.AuthorError = err.Error()
		return
	}
	runtime, err := runtimeVersionAt(nil, block.Hash)
	if err != nil {
		block.SessionError = err.Error()
		block.AuthorError = err.Error()
		return
	}
	m, err := s.getMetadataInstant(runtime.SpecVersion, block.Hash)
	if err != nil {
		block.SessionError = err.Error()
		block.AuthorError = err.Error()
		return
	}
	value, status, err := readPlainState(ctx, block.Hash, runtime.SpecVersion, m, "Session", "CurrentIndex")
	block.SessionStatus = status
	if err != nil {
		block.SessionError = err.Error()
	} else if status == model.StateAvailable {
		index, err := strconv.ParseUint(string(value), 10, 32)
		if err != nil {
			block.SessionStatus = model.StateUnknown
			block.SessionError = "invalid session index: " + err.Error()
		} else {
			v := uint32(index)
			block.SessionIndex = &v
		}
	}
	if len(preRuntime) == 0 {
		block.AuthorError = "missing or ambiguous pre-runtime digest"
		return
	}
	value, status, err = readPlainState(ctx, block.Hash, runtime.SpecVersion, m, "Session", "Validators")
	block.AuthorStatus = status
	if err != nil {
		block.AuthorError = err.Error()
		return
	}
	if status != model.StateAvailable {
		return
	}
	var validators []string
	if err = json.Unmarshal([]byte(value), &validators); err == nil {
		for _, validator := range validators {
			if !address.VerifySubstrateAddress(validator) {
				err = fmt.Errorf("invalid validator account")
				break
			}
		}
	}
	if err == nil {
		block.Validator, err = authorFromDigest(preRuntime, validators)
	}
	if err != nil {
		block.AuthorStatus = model.StateUnknown
		block.AuthorError = err.Error()
	}
}

// Validate byte lengths and indices before indexing. The old SDK helper can
// panic on an out-of-range BABE index or malformed Aura digest.
func authorFromDigest(raw []byte, validators []string) (string, error) {
	var digest substrate.PreRuntime
	if err := json.Unmarshal(raw, &digest); err != nil {
		return "", err
	}
	data, err := hex.DecodeString(strings.TrimPrefix(digest.Data, "0x"))
	if err != nil {
		return "", err
	}
	if len(validators) == 0 {
		return "", fmt.Errorf("empty validator set")
	}
	var index uint64
	switch digest.Engine {
	case substrate.CidAura:
		if len(data) != 8 {
			return "", fmt.Errorf("invalid Aura digest length")
		}
		index = binary.LittleEndian.Uint64(data) % uint64(len(validators))
	case substrate.CidBabe:
		if err := validateQuipBabeDigest(data); err != nil {
			return "", err
		}
		index = uint64(binary.LittleEndian.Uint32(data[1:5]))
	default:
		return "", fmt.Errorf("unsupported author consensus engine %d", digest.Engine)
	}
	if index >= uint64(len(validators)) {
		return "", fmt.Errorf("author index %d exceeds validator set", index)
	}
	if !address.VerifySubstrateAddress(validators[index]) {
		return "", fmt.Errorf("invalid validator account")
	}
	return address.Format(validators[index]), nil
}

// Pinned Quip SDK 845eb998: PreDigest = tag + u32 index + u64 slot;
// primary/secondary-VRF additionally carry sr25519 (32+64 bytes) and a
// fixed 731-byte PQ field. This is the SCALE struct envelope, not the shorter
// variable-length library proof. Classical 109-byte payloads are not Quip VRF
// payloads. See testdata/quip-babe for SDK-generated, verified examples.
// The finalized node validates cryptography; we validate the supported layout
// and the SDK library_proof zero-padding invariant before extracting the index.
func validateQuipBabeDigest(data []byte) error {
	const prefixLen = 1 + 4 + 8
	const nativeVRFLen = 32 + 64
	const pqFieldLen = 731
	const minBindingLen = 1 + 411 // delta byte + rc9 MIN_FALCON512_SIG_LEN
	if len(data) == prefixLen && data[0] == 2 {
		return nil
	}
	if len(data) != prefixLen+nativeVRFLen+pqFieldLen || (data[0] != 1 && data[0] != 3) {
		return fmt.Errorf("unsupported or malformed Quip BABE digest")
	}
	pq := data[prefixLen+nativeVRFLen:]
	bindingLen := minBindingLen + int(pq[0])
	for _, padding := range pq[bindingLen:] {
		if padding != 0 {
			return fmt.Errorf("nonzero Quip BABE VRF padding")
		}
	}
	return nil
}
