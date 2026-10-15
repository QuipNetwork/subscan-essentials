package service

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/itering/subscan/model"
	"github.com/itering/subscan/util/mq"
	substrate "github.com/itering/substrate-api-rpc"
	rpcmodel "github.com/itering/substrate-api-rpc/model"
	"github.com/itering/substrate-api-rpc/websocket"
	"github.com/stretchr/testify/require"
)

type capturedCall struct {
	File           string        `json:"file"`
	Method         string        `json:"method"`
	Params         []interface{} `json:"params"`
	RequestSHA256  string        `json:"requestSha256"`
	ResponseSHA256 string        `json:"responseSha256"`
}

type capturedResponse struct {
	Result         json.RawMessage `json:"result"`
	Error          json.RawMessage `json:"error"`
	TransportError json.RawMessage `json:"transportError"`
}

// Keep raw JSON results: decoding captured u64 weights into float64 would lose
// evidence before the production fee decoder gets to inspect it.
type auxiliaryReplay map[string]capturedResponse

func captureKey(method string, params []interface{}) string {
	raw, _ := json.Marshal(params)
	return method + ":" + fmt.Sprintf("%x", sha256.Sum256(raw))
}

func verifiedCaptureFile(t *testing.T, path, digest string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		compressed, readErr := os.ReadFile(path + ".gz")
		require.NoError(t, readErr)
		reader, unzipErr := gzip.NewReader(bytes.NewReader(compressed))
		require.NoError(t, unzipErr)
		raw, err = io.ReadAll(reader)
		require.NoError(t, reader.Close())
	}
	require.NoError(t, err)
	require.Equal(t, digest, fmt.Sprintf("%x", sha256.Sum256(raw)), path)
	return raw
}

func (r auxiliaryReplay) load(t *testing.T, dir, manifestSHA string) []capturedCall {
	t.Helper()
	var manifest struct {
		Calls []capturedCall `json:"calls"`
	}
	require.NoError(t, json.Unmarshal(verifiedCaptureFile(t, filepath.Join(dir, "capture-manifest.json"), manifestSHA), &manifest))
	for _, call := range manifest.Calls {
		r.add(t, dir, dir, call, false)
	}
	return manifest.Calls
}

func (r auxiliaryReplay) add(t *testing.T, requestDir, responseDir string, call capturedCall, replace bool) {
	t.Helper()
	raw := verifiedCaptureFile(t, filepath.Join(requestDir, call.File+".request.json"), call.RequestSHA256)
	var req struct {
		ID     int
		Method string
		Params []interface{}
	}
	require.NoError(t, json.Unmarshal(raw, &req))
	// WS supplemental entries omit method/params; request bytes remain pinned.
	if call.Method != "" {
		require.Equal(t, call.Method, req.Method)
		require.Equal(t, call.Params, req.Params)
	}
	suffix := ".response.json"
	if replace {
		suffix = ".ws.response.json"
	}
	raw = verifiedCaptureFile(t, filepath.Join(responseDir, call.File+suffix), call.ResponseSHA256)
	var response capturedResponse
	require.NoError(t, json.Unmarshal(raw, &response))
	if len(response.TransportError) == 0 {
		var envelope struct {
			ID      int
			JSONRPC string `json:"jsonrpc"`
		}
		require.NoError(t, json.Unmarshal(raw, &envelope))
		require.Equal(t, req.ID, envelope.ID)
		require.Equal(t, "2.0", envelope.JSONRPC)
		require.True(t, len(response.Result) != 0 || len(response.Error) != 0)
	}
	key := captureKey(req.Method, req.Params)
	if old, exists := r[key]; exists && !replace {
		require.Equal(t, old, response, "conflicting captures for %s", req.Method)
	}
	r[key] = response
}

func (r auxiliaryReplay) lookup(method string, params []interface{}, storageAlias bool) (capturedResponse, bool) {
	response, ok := r[captureKey(method, params)]
	// The older archive captured state_getStorage(key, hash). Adapt ONLY this
	// documented alias; never substitute hashes, keys, runtime or metadata APIs.
	if !ok && storageAlias && method == "state_getStorageAt" {
		response, ok = r[captureKey("state_getStorage", params)]
	}
	return response, ok
}

func (r auxiliaryReplay) serve(t *testing.T, storageAlias bool) func() []executionRequest {
	t.Helper()
	return executionRPC(t, func(method string, params []interface{}) (interface{}, error) {
		response, ok := r.lookup(method, params, storageAlias)
		if !ok {
			// Unexpected auxiliary calls may be nonfatal in production. Fail the
			// test here as well, so graceful unknown cannot hide a bad replay.
			t.Errorf("uncaptured RPC %s (params SHA256 key %s)", method, captureKey(method, params))
			return nil, fmt.Errorf("uncaptured RPC %s", method)
		}
		if len(response.Error) != 0 || len(response.TransportError) != 0 {
			return nil, fmt.Errorf("captured RPC/transport failure: %s%s", response.Error, response.TransportError)
		}
		return response.Result, nil
	})
}

func TestCapturedPrimaryBabeEnrichment(t *testing.T) {
	isolateRuntimeSpecs(t, 118)
	dir := filepath.Join("..", "..", "testdata", "quip-babe", "primary-aux")
	replay := auxiliaryReplay{}
	require.Len(t, replay.load(t, dir, "993d42411200c3fb8ff2dc12b939acfae3665670173ab6757542019adcbd6d9e"), 6)
	calls := replay.serve(t, false)
	var archived rpcmodel.BlockResult
	require.NoError(t, json.Unmarshal(babeFixture(t, "aglais-591853.json"), &archived))
	const hash = "0xe5e55f20230e3e8f730ef47e54cd265e1aa9b1271cd506131231108d5695caa9"
	identity, ok := replay.lookup("chain_getBlockHash", []interface{}{591853}, false)
	require.True(t, ok)
	require.JSONEq(t, `"`+hash+`"`, string(identity.Result))
	logs, err := substrate.DecodeLogDigest(archived.Block.Header.Digest.Logs)
	require.NoError(t, err)
	var digest []byte
	count := 0
	for _, log := range logs {
		if strings.EqualFold(log.Type, "PreRuntime") {
			count++
			digest, err = json.Marshal(log.Value)
			require.NoError(t, err)
		}
	}
	require.Equal(t, 1, count)
	block := &model.ChainBlock{BlockNum: 591853, Hash: hash, ParentHash: archived.Block.Header.ParentHash, SpecVersion: 118}
	(&Service{dao: newExecutionDAO(t)}).enrichBlockState(context.Background(), block, digest)
	require.Equal(t, model.StateAvailable, block.SessionStatus, block.SessionError)
	require.NotNil(t, block.SessionIndex)
	require.Zero(t, *block.SessionIndex)
	require.Equal(t, model.StateAvailable, block.AuthorStatus, block.AuthorError)
	require.Equal(t, "d88854de054c7534450cddace65332a98d12ba06ff3adb39f9ad40f129b37aa7", block.Validator)
	require.Equal(t, hash, block.SessionStateHash)
	require.Equal(t, hash, block.AuthorStateHash)
	require.Len(t, calls(), 4, "runtime, metadata and both storage reads at exact block hash")
	api, err := json.Marshal((&Service{}).BlockAsSampleJson(block))
	require.NoError(t, err)
	var fields map[string]interface{}
	require.NoError(t, json.Unmarshal(api, &fields))
	require.Equal(t, float64(0), fields["session_index"])
	require.Equal(t, model.StateAvailable, fields["author_status"])
}

func TestCapturedAuxiliaryBlockReplay(t *testing.T) {
	root := os.Getenv("QUIP_AUXILIARY_CAPTURES")
	if root == "" {
		t.Skip("set QUIP_AUXILIARY_CAPTURES to the SHA-pinned rr archive with responses/ and responses-ws/")
	}
	replay := auxiliaryReplay{}
	require.Len(t, replay.load(t, filepath.Join(root, "responses"), "f19329bf8e57e616b472ec75f70557d0118051fd169a497585b9042f3e79474a"), 144)
	var wsCalls []capturedCall
	wsDir := filepath.Join(root, "responses-ws")
	require.NoError(t, json.Unmarshal(verifiedCaptureFile(t, filepath.Join(wsDir, "ws-manifest.json"), "d94302fda681a585bddbc5832f4a95b220772ebb8c87e9911b90b0cba5c45ad1"), &wsCalls))
	require.Len(t, wsCalls, 3)
	for _, call := range wsCalls {
		replay.add(t, filepath.Join(root, "responses"), wsDir, call, true)
	}
	websocket.SetChannelPoolMaxCap(1)
	t.Cleanup(func() { websocket.SetChannelPoolMaxCap(25) })
	totalSigned, totalFailed := 0, 0
	for _, tc := range []struct {
		number                   uint
		spec, extrinsics, events int
	}{
		{330109, 117, 8, 36}, {330110, 118, 5, 21}, {591863, 118, 5, 20}, {591865, 118, 5, 18},
	} {
		t.Run(fmt.Sprint(tc.number), func(t *testing.T) {
			isolateRuntimeSpecs(t, 117, 118)
			calls := replay.serve(t, true)
			d := newExecutionDAO(t)
			oldQueue := mq.Instant
			mq.Instant = &blockIntegrityQueue{dao: &d.blockIntegrityDao}
			t.Cleanup(func() { mq.Instant = oldQueue })
			s := &Service{dao: d}
			require.NoError(t, s.FillBlockData(context.Background(), tc.number, false))
			require.True(t, d.committed)
			require.Len(t, d.extrinsics, tc.extrinsics)
			require.Len(t, d.events, tc.events)
			block := d.blocks[tc.number]
			require.Equal(t, tc.spec, block.SpecVersion)
			require.Equal(t, model.StateAvailable, block.SessionStatus, block.SessionError)
			require.NotNil(t, block.SessionIndex)
			require.Zero(t, *block.SessionIndex, "captured null + metadata Default is observed zero")
			require.Equal(t, model.StateAvailable, block.AuthorStatus, block.AuthorError)
			require.Equal(t, block.Hash, block.SessionStateHash)
			require.Equal(t, block.Hash, block.AuthorStateHash)
			// Independently select the account from the captured SCALE vector and
			// BABE digest, without the production author/validator decoder.
			result, ok := replay.lookup("chain_getBlock", []interface{}{block.Hash}, false)
			require.True(t, ok)
			var rawBlock rpcmodel.BlockResult
			require.NoError(t, json.Unmarshal(result.Result, &rawBlock))
			logs, err := substrate.DecodeLogDigest(rawBlock.Block.Header.Digest.Logs)
			require.NoError(t, err)
			var authority uint32
			for _, log := range logs {
				if strings.EqualFold(log.Type, "PreRuntime") {
					raw, err := json.Marshal(log.Value)
					require.NoError(t, err)
					var digest substrate.PreRuntime
					require.NoError(t, json.Unmarshal(raw, &digest))
					data, err := hex.DecodeString(strings.TrimPrefix(digest.Data, "0x"))
					require.NoError(t, err)
					require.Len(t, data, 13)
					authority = binary.LittleEndian.Uint32(data[1:5])
				}
			}
			for _, call := range calls() {
				if call.Method == "state_getStorageAt" && call.Params[0] == "0xcec5070d609dd3497f72bde07fc96ba088dcde934c658227ee1dfafcd6e16903" {
					require.Equal(t, block.Hash, call.Params[1])
					value, ok := replay.lookup(call.Method, call.Params, true)
					require.True(t, ok)
					var encoded string
					require.NoError(t, json.Unmarshal(value.Result, &encoded))
					require.Len(t, encoded, 196)
					require.True(t, strings.HasPrefix(encoded, "0x0c"))
					require.Less(t, authority, uint32(3))
					require.Equal(t, encoded[4+authority*64:4+(authority+1)*64], block.Validator)
				}
			}
			actualFees := map[string]string{}
			outcomes := map[string]bool{}
			for _, event := range d.events {
				if event.Phase != 0 {
					continue
				}
				if strings.EqualFold(event.ModuleId, "System") && (event.EventId == "ExtrinsicSuccess" || event.EventId == "ExtrinsicFailed") {
					_, exists := outcomes[event.ExtrinsicIndex]
					require.False(t, exists)
					outcomes[event.ExtrinsicIndex] = event.EventId == "ExtrinsicSuccess"
				}
				if !strings.EqualFold(event.ModuleId, "TransactionPayment") || event.EventId != "TransactionFeePaid" {
					continue
				}
				// Persisted params are raw SCALE, not hydrated Params. Independently
				// inspect this captured event's AccountId32 + u128 fee + u128 tip.
				require.Len(t, event.ParamsRawBytes, 64)
				fee := make([]byte, 16)
				for i := range fee {
					fee[15-i] = event.ParamsRawBytes[32+i]
				}
				_, exists := actualFees[event.ExtrinsicIndex]
				require.False(t, exists)
				actualFees[event.ExtrinsicIndex] = new(big.Int).SetBytes(fee).String()
			}
			for _, ext := range d.extrinsics {
				outcome, exists := outcomes[ext.ExtrinsicIndex]
				require.True(t, exists)
				require.Equal(t, outcome, ext.Success)
				if !ext.Success {
					totalFailed++
				}
				if !ext.IsSigned {
					require.Equal(t, model.FeeNotApplicable, ext.FeeStatus)
					continue
				}
				totalSigned++
				require.True(t, strings.HasPrefix(ext.Signature, "0x"))
				require.Len(t, ext.Signature, 3322)
				require.Equal(t, model.FeeEstimated, ext.FeeStatus, ext.FeeError)
				require.Equal(t, model.FeeAvailable, ext.UsedFeeStatus, ext.FeeError)
				info := ext.FeeInfo()
				require.Equal(t, block.ParentHash, info.FeeStateHash)
				require.Equal(t, block.Hash, info.UsedFeeStateHash)
				actual, exists := actualFees[ext.ExtrinsicIndex]
				require.True(t, exists)
				require.Equal(t, actual, info.UsedFee.String())
				for i, raw := range rawBlock.Block.Extrinsics {
					if ext.ExtrinsicIndex != fmt.Sprintf("%d-%d", tc.number, i) {
						continue
					}
					response, ok := replay.lookup("payment_queryFeeDetails", []interface{}{raw, block.ParentHash}, false)
					require.True(t, ok)
					var fee PaymentQueryFeeDetails
					require.NoError(t, json.Unmarshal(response.Result, &fee))
					require.True(t, fee.EstimateFee().Equal(*info.Fee))
				}
			}
			before := len(calls())
			require.NoError(t, s.FillBlockData(context.Background(), tc.number, false))
			require.Len(t, calls(), before, "committed retry performs no historical RPC")
			require.Len(t, d.extrinsics, tc.extrinsics)
			require.Len(t, d.events, tc.events)
		})
	}
	require.Equal(t, 19, totalSigned)
	require.Equal(t, 11, totalFailed)
	// Exercise the captured oversized fee response through the production
	// query-info decoder too; the u64 proof size must survive without rounding.
	t.Run("large weight WS response", func(t *testing.T) {
		replay.serve(t, true)
		call := wsCalls[1]
		var req struct{ Params []string }
		require.NoError(t, json.Unmarshal(verifiedCaptureFile(t, filepath.Join(root, "responses", call.File+".request.json"), call.RequestSHA256), &req))
		info, err := GetPaymentQueryInfo(context.Background(), 117, req.Params[0], req.Params[1], true)
		require.NoError(t, err)
		require.Equal(t, uint64(14941862699704748992), info.ProofSize)
		require.Equal(t, "1620600608375", info.PartialFee.String())
	})
}
