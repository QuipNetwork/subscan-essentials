// Package state resolves runtime metadata for an explicit state hash.
package state

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	scalecodec "github.com/itering/scale.go"
	"github.com/itering/substrate-api-rpc/metadata"
	rpcmodel "github.com/itering/substrate-api-rpc/model"
	"github.com/itering/substrate-api-rpc/rpc"
	"github.com/itering/substrate-api-rpc/websocket"
)

func RuntimeVersionAt(conn websocket.WsConn, hash string) (*rpcmodel.RuntimeVersion, error) {
	v := &rpcmodel.JsonRpcResult{}
	if err := websocket.SendWsRequest(conn, v, rpc.StateGetRuntimeVersion(19401, hash)); err != nil {
		return nil, err
	}
	// Do not use ToRuntimeVersion's unchecked map assertion or infer a missing
	// runtime from the latest spec/nearby block (unsafe at upgrade boundaries).
	var required struct {
		SpecVersion *int `json:"specVersion"`
	}
	if err := v.CheckErr(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(v.Result)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(encoded, &required); err != nil {
		return nil, err
	}
	if required.SpecVersion == nil || *required.SpecVersion < 0 {
		return nil, fmt.Errorf("missing or invalid specVersion at %s", hash)
	}
	var runtime rpcmodel.RuntimeVersion
	if err := json.Unmarshal(encoded, &runtime); err != nil {
		return nil, err
	}
	return &runtime, nil
}

func RegisterCheckedMetadata(raw *metadata.RuntimeRaw) (instant *metadata.Instant, err error) {
	// The dependency registration helper ignores decoding errors and can panic.
	// Validate before publishing anything in its global runtime cache.
	defer func() {
		if recovered := recover(); recovered != nil {
			instant = nil
			err = fmt.Errorf("metadata spec %d: %v", raw.Spec, recovered)
		}
	}()
	data, err := hex.DecodeString(strings.TrimPrefix(raw.Raw, "0x"))
	if err != nil {
		return nil, fmt.Errorf("metadata spec %d hex: %w", raw.Spec, err)
	}
	decoder := scalecodec.MetadataDecoder{}
	decoder.Init(data)
	if err := decoder.Process(); err != nil {
		return nil, fmt.Errorf("metadata spec %d: %w", raw.Spec, err)
	}
	if decoder.Data.GetRemainingLength() != 0 {
		return nil, fmt.Errorf("metadata spec %d has trailing bytes", raw.Spec)
	}
	return metadata.RegNewMetadataType(raw.Spec, raw.Raw), nil
}

func MetadataAt(spec int, hash string) (*metadata.Instant, error) {
	if hash == "" {
		return nil, fmt.Errorf("explicit state hash required")
	}
	if m := metadata.RuntimeMetadata[spec]; m != nil {
		return m, nil
	}
	raw, err := rpc.GetMetadataByHash(nil, hash)
	if err != nil {
		return nil, err
	}
	return RegisterCheckedMetadata(&metadata.RuntimeRaw{Spec: spec, Raw: raw})
}
