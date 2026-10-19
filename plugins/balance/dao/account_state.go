package dao

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/itering/scale.go/types"
	"github.com/itering/scale.go/types/scaleBytes"
	"github.com/itering/subscan-plugin/storage"
	core "github.com/itering/subscan/model"
	bModel "github.com/itering/subscan/plugins/balance/model"
	"github.com/itering/subscan/share/state"
	"github.com/itering/subscan/util/address"
	"github.com/itering/substrate-api-rpc/hasher"
	"github.com/itering/substrate-api-rpc/metadata"
	rpcmodel "github.com/itering/substrate-api-rpc/model"
	"github.com/itering/substrate-api-rpc/rpc"
	"github.com/itering/substrate-api-rpc/websocket"
	"github.com/shopspring/decimal"
)

// ReadAccountState deliberately ignores Block.SpecVersion: that is the execution
// runtime, which may differ from the runtime installed in this block's state.
// Auxiliary failures become explicit unknown observations, not projection errors.
func ReadAccountState(ctx context.Context, block *storage.Block, accountID string) (out bModel.AccountState) {
	out.StateStatus = core.StateUnknown
	if block != nil {
		out.StateHash = block.Hash
		if block.BlockNum >= 0 {
			n := uint(block.BlockNum)
			out.StateBlockNum = &n
		}
	}
	defer func() {
		if r := recover(); r != nil {
			out.Nonce, out.Balance, out.Locked, out.Reserved = nil, nil, nil, nil
			out.StateStatus = core.StateUnknown
			out.StateError = fmt.Sprintf("account state decode: %v", r)
		}
	}()
	fail := func(err error) bModel.AccountState { out.StateError = err.Error(); return out }
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	hash, err := hex.DecodeString(strings.TrimPrefix(out.StateHash, "0x"))
	if err != nil || len(hash) != 32 || bytes.Equal(hash, make([]byte, 32)) || block == nil || !block.Finalized || out.StateBlockNum == nil {
		return fail(fmt.Errorf("explicit finalized block state required"))
	}
	if !address.VerifySubstrateAddress(accountID) {
		return fail(fmt.Errorf("invalid AccountId32"))
	}
	runtime, err := state.RuntimeVersionAt(nil, out.StateHash)
	if err != nil {
		return fail(err)
	}
	out.StateSpecVersion = &runtime.SpecVersion
	m, err := state.MetadataAt(runtime.SpecVersion, out.StateHash)
	if err != nil {
		return fail(err)
	}
	entry, prefix, valueType, hashName, err := accountStorage(m, runtime.SpecVersion)
	if err != nil {
		return fail(err)
	}
	accountBytes, _ := hex.DecodeString(strings.TrimPrefix(accountID, "0x"))
	key := append(prefix, hasher.HashByCryptoName(accountBytes, hashName)...)
	var response struct {
		Result json.RawMessage `json:"result"`
		Error  *rpcmodel.Error `json:"error"`
	}
	if err := websocket.SendWsRequest(nil, &response, rpc.StateGetStorage(19402, "0x"+hex.EncodeToString(key), out.StateHash)); err != nil {
		return fail(err)
	}
	if response.Error != nil {
		return fail(fmt.Errorf("account storage RPC %d: %s", response.Error.Code, response.Error.Message))
	}
	if len(response.Result) == 0 {
		return fail(fmt.Errorf("missing account storage result"))
	}
	var raw string
	if bytes.Equal(bytes.TrimSpace(response.Result), []byte("null")) {
		switch entry.Modifier {
		case "Optional":
			out.StateStatus = core.StateAbsent
			return out
		case "Default":
			raw = entry.Fallback
		default:
			return fail(fmt.Errorf("unknown account storage modifier %q", entry.Modifier))
		}
	} else if err := json.Unmarshal(response.Result, &raw); err != nil {
		return fail(err)
	}
	data, err := hex.DecodeString(strings.TrimPrefix(raw, "0x"))
	if err != nil || len(data) == 0 {
		return fail(fmt.Errorf("invalid account storage bytes"))
	}
	opt := &types.ScaleDecoderOption{Spec: runtime.SpecVersion, Metadata: (*types.MetadataStruct)(m)}
	decoder := types.ScaleDecoder{}
	decoder.Init(scaleBytes.ScaleBytes{Data: data}, opt)
	value := decoder.ProcessAndUpdateData(valueType)
	if decoder.Data.GetRemainingLength() != 0 {
		return fail(fmt.Errorf("account storage has trailing bytes"))
	}
	// Older codec versions pad truncated integers. Exact re-encoding also rejects
	// those values and noncanonical encodings without guessing an AccountInfo size.
	encoded := types.EncodeWithOpt(valueType, value, opt)
	if !strings.EqualFold(encoded, hex.EncodeToString(data)) {
		return fail(fmt.Errorf("account storage does not round-trip exactly"))
	}
	rawJSON, err := json.Marshal(value)
	if err != nil {
		return fail(err)
	}
	parsed, err := parseAccountData(rawJSON)
	if err != nil {
		return fail(err)
	}
	out.Nonce, out.Balance, out.Locked, out.Reserved = parsed.Nonce, parsed.Balance, parsed.Locked, parsed.Reserved
	out.StateStatus = core.StateAvailable
	return out
}

// Only a single AccountId32 map is supported, with explicitly supported hashers.
// Encoding the key and decoding the value both use this exact metadata instance.
func accountStorage(m *metadata.Instant, spec int) (*types.MetadataStorage, []byte, string, string, error) {
	for _, module := range m.Metadata.Modules {
		if module.Name != "System" {
			continue
		}
		for _, field := range module.Storage {
			if field.Name != "Account" || module.Prefix == "" {
				continue
			}
			var keyType, valueType, hashName string
			switch field.Type.Origin {
			case "MapType":
				if field.Type.MapType != nil {
					keyType, valueType, hashName = field.Type.MapType.Key, field.Type.MapType.Value, field.Type.MapType.Hasher
				}
			case "Map":
				n := field.Type.NMapType
				if n != nil && len(n.KeyVec) == 1 && len(n.Hashers) == 1 {
					keyType, valueType, hashName = n.KeyVec[0], n.Value, n.Hashers[0]
				}
			}
			// Check the actual key codec as portable metadata type names need not be AccountId.
			probe := strings.Repeat("ab", 32)
			if keyType == "" || valueType == "" || types.EncodeWithOpt(keyType, probe, &types.ScaleDecoderOption{Spec: spec, Metadata: (*types.MetadataStruct)(m)}) != probe {
				return nil, nil, "", "", fmt.Errorf("unsupported System.Account key/value schema")
			}
			switch hashName {
			case "Blake2_128Concat", "Twox64Concat", "Identity", "Blake2_128", "Blake2_256", "Twox128":
			default:
				return nil, nil, "", "", fmt.Errorf("unsupported account hasher %q", hashName)
			}
			prefix := append(hasher.HashByCryptoName([]byte(module.Prefix), "Twox128"), hasher.HashByCryptoName([]byte(field.Name), "Twox128")...)
			return &field, prefix, valueType, hashName, nil
		}
	}
	return nil, nil, "", "", fmt.Errorf("System.Account metadata unavailable")
}

func parseAccountData(raw []byte) (bModel.AccountState, error) {
	var object struct {
		Nonce json.RawMessage            `json:"nonce"`
		Data  map[string]json.RawMessage `json:"data"`
	}
	var out bModel.AccountState
	if err := json.Unmarshal(raw, &object); err != nil {
		return out, err
	}
	unsigned := func(raw json.RawMessage, bits int) (decimal.Decimal, error) {
		s := string(raw)
		if len(s) > 0 && s[0] == '"' {
			if err := json.Unmarshal(raw, &s); err != nil {
				return decimal.Zero, err
			}
		}
		if s == "" {
			return decimal.Zero, fmt.Errorf("missing unsigned account value")
		}
		for _, c := range s {
			if c < '0' || c > '9' {
				return decimal.Zero, fmt.Errorf("invalid unsigned account value")
			}
		}
		n, err := decimal.NewFromString(s)
		if err != nil || n.GreaterThan(decimal.New(2, 0).Pow(decimal.NewFromInt(int64(bits))).Sub(decimal.NewFromInt(1))) {
			return decimal.Zero, fmt.Errorf("account value exceeds u%d", bits)
		}
		return n, nil
	}
	nonce, err := unsigned(object.Nonce, 32)
	if err != nil {
		return out, err
	}
	n, err := strconv.ParseUint(nonce.String(), 10, 32)
	if err != nil {
		return out, err
	}
	v := uint32(n)
	out.Nonce = &v
	free, err := unsigned(object.Data["free"], 128)
	if err != nil {
		return out, err
	}
	reserved, err := unsigned(object.Data["reserved"], 128)
	if err != nil {
		return out, err
	}
	var locked decimal.Decimal
	if raw, ok := object.Data["frozen"]; ok {
		locked, err = unsigned(raw, 128)
	} else {
		field := func(camel, snake string) json.RawMessage {
			if v, ok := object.Data[camel]; ok {
				return v
			}
			return object.Data[snake]
		}
		var misc, fee decimal.Decimal
		misc, err = unsigned(field("miscFrozen", "misc_frozen"), 128)
		if err == nil {
			fee, err = unsigned(field("feeFrozen", "fee_frozen"), 128)
		}
		locked = decimal.Max(misc, fee)
	}
	if err != nil {
		return out, err
	}
	total := free.Add(reserved)
	out.Balance, out.Locked, out.Reserved = &total, &locked, &reserved
	return out, nil
}
