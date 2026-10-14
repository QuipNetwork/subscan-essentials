package service

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	scalecodec "github.com/itering/scale.go"
	"github.com/itering/subscan/util"
	"github.com/itering/substrate-api-rpc/metadata"
	rpcmodel "github.com/itering/substrate-api-rpc/model"
	"github.com/itering/substrate-api-rpc/rpc"
	"github.com/itering/substrate-api-rpc/websocket"
)

// executionStateHash also guards the direct CreateChainBlock entry point. Genesis
// has no executable parent state; its metadata is read at the genesis hash.
func executionStateHash(hash string, block *rpcmodel.Block) (string, uint, error) {
	if block == nil {
		return "", 0, fmt.Errorf("nil block data")
	}
	number, err := strconv.ParseUint(strings.TrimPrefix(block.Header.Number, "0x"), 16, strconv.IntSize)
	if err != nil {
		return "", 0, fmt.Errorf("invalid block number %q: %w", block.Header.Number, err)
	}
	if number == 0 {
		if hash == "" {
			return "", 0, fmt.Errorf("missing genesis hash")
		}
		return hash, 0, nil
	}
	parent := block.Header.ParentHash
	raw, err := hex.DecodeString(strings.TrimPrefix(parent, "0x"))
	if err != nil || len(raw) != 32 || strings.Trim(strings.TrimPrefix(parent, "0x"), "0") == "" {
		return "", uint(number), fmt.Errorf("block %d invalid execution parent hash %q", number, parent)
	}
	return parent, uint(number), nil
}

func runtimeVersionAt(conn websocket.WsConn, hash string) (*rpcmodel.RuntimeVersion, error) {
	v := &rpcmodel.JsonRpcResult{}
	if err := websocket.SendWsRequest(conn, v, rpc.StateGetRuntimeVersion(wsSpec, hash)); err != nil {
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

func (s *Service) regRuntimeVersion(ctx context.Context, name string, spec int, blockNum uint, hash string) error {
	// Resolve and validate metadata before recording a runtime as usable. The
	// database entry tracks its state height, independently of block execution.
	raw, err := s.runtimeMetadataRaw(spec, hash)
	if err != nil {
		return err
	}
	instant := metadata.RuntimeMetadata[spec]
	if instant == nil {
		instant, err = registerCheckedMetadata(raw)
		if err != nil {
			return err
		}
	}
	if s.dao.CreateRuntimeVersion(ctx, name, spec, blockNum) {
		s.setRuntimeData(spec, instant, raw.Raw)
	}
	return nil
}

func (s *Service) regCodecMetadata(hash ...string) string {
	if coded, err := rpc.GetMetadataByHash(nil, hash...); err == nil {
		return coded
	}
	return ""
}

func (s *Service) setRuntimeData(spec int, runtime *metadata.Instant, rawData string) {
	var modules []string
	for _, value := range runtime.Metadata.Modules {
		modules = append(modules, value.Name)
	}
	s.dao.SetRuntimeData(spec, strings.Join(modules, "|"), rawData)
}

func (s *Service) runtimeMetadataRaw(spec int, hash string) (*metadata.RuntimeRaw, error) {
	if raw := s.dao.RuntimeVersionRaw(spec); raw != nil && raw.Raw != "" {
		if raw.Spec != spec {
			return nil, fmt.Errorf("metadata spec %d does not match requested %d", raw.Spec, spec)
		}
		return raw, nil
	}
	if hash == "" {
		return nil, fmt.Errorf("missing state hash for runtime %d metadata", spec)
	}
	raw, err := rpc.GetMetadataByHash(nil, hash)
	if err != nil {
		return nil, fmt.Errorf("metadata at %s: %w", hash, err)
	}
	if raw == "" {
		return nil, fmt.Errorf("empty metadata at %s", hash)
	}
	return &metadata.RuntimeRaw{Spec: spec, Raw: raw}, nil
}

func (s *Service) getMetadataInstant(spec int, hash string) (*metadata.Instant, error) {
	if instant := metadata.RuntimeMetadata[spec]; instant != nil {
		return instant, nil
	}
	raw, err := s.runtimeMetadataRaw(spec, hash)
	if err != nil {
		return nil, err
	}
	return registerCheckedMetadata(raw)
}

func registerCheckedMetadata(raw *metadata.RuntimeRaw) (instant *metadata.Instant, err error) {
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

func (s *Service) registerBlockRuntimes(ctx context.Context, conn websocket.WsConn, blockNum uint, blockHash, executionHash string) (*rpcmodel.RuntimeVersion, error) {
	// The body and emitted events were executed by the parent-state runtime.
	// An upgrade installed in this block describes execution of its children.
	execution, err := runtimeVersionAt(conn, executionHash)
	if err != nil {
		return nil, fmt.Errorf("block %d execution runtime: %w", blockNum, err)
	}
	post := execution
	if blockNum > 0 {
		post, err = runtimeVersionAt(conn, blockHash)
		if err != nil {
			return nil, fmt.Errorf("block %d post-state runtime: %w", blockNum, err)
		}
	}
	// Register an installed runtime separately; never use it for this body's
	// decoding. ChainBlock.SpecVersion always records the execution runtime.
	if post.SpecVersion != execution.SpecVersion {
		if err = s.regRuntimeVersion(ctx, post.ImplName, post.SpecVersion, blockNum, blockHash); err != nil {
			return nil, fmt.Errorf("block %d post-state metadata: %w", blockNum, err)
		}
	}
	executionStateNum := blockNum
	if blockNum > 0 {
		executionStateNum--
	}
	if err = s.regRuntimeVersion(ctx, execution.ImplName, execution.SpecVersion, executionStateNum, executionHash); err != nil {
		return nil, fmt.Errorf("block %d execution metadata: %w", blockNum, err)
	}
	if post.SpecVersion > util.CurrentRuntimeSpecVersion {
		util.CurrentRuntimeSpecVersion = post.SpecVersion
	}

	return execution, nil
}
