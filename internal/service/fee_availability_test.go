package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/itering/subscan/util"
	substrate "github.com/itering/substrate-api-rpc"
	"github.com/itering/substrate-api-rpc/metadata"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/itering/subscan/model"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func feePaid(value, tip interface{}) model.ChainEvent {
	return model.ChainEvent{Phase: 0, ModuleId: "TransactionPayment", EventId: "TransactionFeePaid", Params: model.EventParams{{Value: "account"}, {Value: value}, {Value: tip}}}
}
func TestFeeAvailability(t *testing.T) {
	parent, hash := "0x"+strings.Repeat("a", 64), "0x"+strings.Repeat("b", 64)
	var mode atomic.Value
	mode.Store("unavailable")
	calls := executionRPC(t, func(method string, params []interface{}) (interface{}, error) {
		if len(params) != 2 || params[0] != "0x0484" && params[0] != "0x0445" || params[1] != parent {
			return nil, fmt.Errorf("unexpected fee query %s %v", method, params)
		}
		switch mode.Load().(string) {
		case "details":
			if method == "payment_queryFeeDetails" {
				return json.RawMessage(`{"inclusionFee":{"baseFee":"0x0a","lenFee":"20","adjustedWeightFee":30}}`), nil
			}
		case "zero":
			if method == "payment_queryFeeDetails" {
				return json.RawMessage(`{"inclusionFee":null}`), nil
			}
		case "fallback":
			if method == "payment_queryInfo" {
				return json.RawMessage(`{"partialFee":9007199254740993,"weight":{"ref_time":123,"proof_size":14941862699704748992},"class":"normal"}`), nil
			}
		case "malformed":
			return json.RawMessage(`{"inclusionFee":{},"partialFee":null}`), nil
		}
		return nil, fmt.Errorf("unavailable historical fee state")
	})
	for _, tc := range []struct {
		name, mode, raw                  string
		events                           []model.ChainEvent
		feeStatus, usedStatus, fee, used string
	}{
		{"event survives query failure", "unavailable", "0x0484", []model.ChainEvent{feePaid("100", "7")}, model.FeeUnknown, model.FeeAvailable, "", "100"},
		{"event zero survives query failure", "unavailable", "0x0484", []model.ChainEvent{feePaid("0", "0")}, model.FeeUnknown, model.FeeAvailable, "", "0"},
		{"all unknown", "unavailable", "0x0484", nil, model.FeeUnknown, model.FeeUnknown, "", ""},
		{"estimate with observed fee", "details", "0x0484", []model.ChainEvent{feePaid("100", "7")}, model.FeeEstimated, model.FeeAvailable, "60", "100"},
		{"estimate does not imply actual", "details", "0x0484", nil, model.FeeEstimated, model.FeeUnknown, "60", ""},
		{"real zero estimate", "zero", "0x0484", nil, model.FeeEstimated, model.FeeUnknown, "0", ""},
		{"lossless fallback", "fallback", "0x0484", nil, model.FeeEstimated, model.FeeUnknown, "9007199254740993", ""},
		{"malformed is unknown", "malformed", "0x0484", []model.ChainEvent{feePaid("100", "7")}, model.FeeUnknown, model.FeeAvailable, "", "100"},
		{"bad event is unknown", "details", "0x0484", []model.ChainEvent{feePaid("bad", "0")}, model.FeeEstimated, model.FeeUnknown, "60", ""},
		{"bare does not query", "unavailable", "0x0405", nil, model.FeeNotApplicable, model.FeeNotApplicable, "", ""},
		{"general may pay", "details", "0x0445", nil, model.FeeEstimated, model.FeeUnknown, "60", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mode.Store(tc.mode)
			before := len(calls())
			d := newExecutionDAO(t)
			block := &model.ChainBlock{BlockNum: 42, Hash: hash, ParentHash: parent, SpecVersion: 117}
			events := map[string][]model.ChainEvent{"42-0": tc.events}
			e := model.ChainExtrinsic{IsSigned: tc.raw == "0x0484", CallModule: "example", CallModuleFunction: "test"}
			require.NoError(t, (&Service{dao: d}).createExtrinsic(context.Background(), nil, block, []model.ChainExtrinsic{e}, []string{tc.raw}, events))
			stored := d.extrinsics[4200000]
			require.Equal(t, tc.feeStatus, stored.FeeStatus)
			require.Equal(t, tc.usedStatus, stored.UsedFeeStatus)
			info := stored.FeeInfo()
			if tc.fee == "" {
				require.Nil(t, info.Fee)
			} else {
				require.Equal(t, tc.fee, info.Fee.String())
			}
			if tc.used == "" {
				require.Nil(t, info.UsedFee)
			} else {
				require.Equal(t, tc.used, info.UsedFee.String())
				require.Equal(t, hash, info.UsedFeeStateHash)
			}
			if tc.feeStatus == model.FeeNotApplicable {
				require.Len(t, calls(), before)
			} else {
				require.Equal(t, parent, info.FeeStateHash)
				require.Greater(t, len(calls()), before)
			}
			if tc.mode == "unavailable" && tc.feeStatus != model.FeeNotApplicable || tc.mode == "malformed" || tc.name == "bad event is unknown" {
				require.NotEmpty(t, info.FeeError)
			}
			raw, err := json.Marshal((&Service{}).ExtrinsicsAsJson(&stored))
			require.NoError(t, err)
			var api map[string]interface{}
			require.NoError(t, json.Unmarshal(raw, &api))
			if tc.fee == "" {
				require.Nil(t, api["fee"])
			} else {
				require.Equal(t, tc.fee, api["fee"])
			}
			if tc.used == "" {
				require.Nil(t, api["used_fee"])
			} else {
				require.Equal(t, tc.used, api["used_fee"])
			}
		})
	}
	mode.Store("fallback")
	info, err := GetPaymentQueryInfo(context.Background(), 117, "0x0484", parent, true)
	require.NoError(t, err)
	require.Equal(t, uint64(14941862699704748992), info.ProofSize)
	require.Equal(t, "123", info.Weight.String())
	before := len(calls())
	_, err = GetPaymentQueryInfo(context.Background(), 117, "0x0484", "", true)
	require.ErrorContains(t, err, "explicit state hash")
	require.Len(t, calls(), before)
	_, _, err = GetExtrinsicFee(context.Background(), "0x0484", parent, 117, decimal.Zero, decimal.NewFromInt(-1), true)
	require.ErrorContains(t, err, "actual fee unavailable")
	mode.Store("unavailable")
	_, actual, err := GetExtrinsicFee(context.Background(), "0x0484", parent, 117, decimal.Zero, decimal.NewFromInt(100), true)
	require.Error(t, err)
	require.Equal(t, "100", actual.String())
}

func TestFeeRPCValidation(t *testing.T) {
	for _, body := range []string{`{}`, `null`, `{"inclusionFee":{}}`, `{"inclusionFee":{"baseFee":-1,"lenFee":0,"adjustedWeightFee":0}}`, `{"inclusionFee":{"baseFee":0.5,"lenFee":0,"adjustedWeightFee":0}}`} {
		var result PaymentQueryFeeDetails
		require.Error(t, json.Unmarshal([]byte(body), &result), body)
	}
	for _, body := range []string{`{}`, `{"partialFee":0,"class":"normal","weight":null}`, `{"partialFee":0,"class":"normal","weight":{"ref_time":1,"proof_size":18446744073709551616}}`} {
		var result PaymentQueryInfo
		require.Error(t, json.Unmarshal([]byte(body), &result), body)
	}
}

func TestArchivedFeeEvents(t *testing.T) {
	root := os.Getenv("QUIP_CONFORMANCE_FIXTURES")
	if root == "" {
		t.Skip("requires reviewed decoder forks and QUIP_CONFORMANCE_FIXTURES")
	}
	isolateRuntimeSpecs(t, 117, 118, 119)
	totalEvents, observedFees := 0, 0
	for _, network := range []string{"aglais-117-118", "local-119"} {
		dir := filepath.Join(root, network)
		manifestBytes, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
		require.NoError(t, err)
		var manifest struct {
			Blocks []struct {
				EventsFile           string `json:"eventsFile"`
				MetadataFile         string `json:"metadataFile"`
				ExecutionSpecVersion int    `json:"executionSpecVersion"`
			} `json:"blocks"`
			Files map[string]struct {
				SHA256 string `json:"sha256"`
			} `json:"files"`
		}
		require.NoError(t, json.Unmarshal(manifestBytes, &manifest))
		read := func(name string) []byte {
			data, err := os.ReadFile(filepath.Join(dir, name))
			require.NoError(t, err)
			require.Equal(t, manifest.Files[name].SHA256, fmt.Sprintf("%x", sha256.Sum256(data)))
			return data
		}
		for _, block := range manifest.Blocks {
			m, err := registerCheckedMetadata(&metadata.RuntimeRaw{Spec: block.ExecutionSpecVersion, Raw: "0x" + hex.EncodeToString(read(block.MetadataFile))})
			require.NoError(t, err)
			decoded, err := substrate.DecodeEvent("0x"+hex.EncodeToString(read(block.EventsFile)), m, block.ExecutionSpecVersion)
			require.NoError(t, err)
			var events []model.ChainEvent
			require.NoError(t, util.UnmarshalAny(&events, decoded))
			totalEvents += len(events)
			byIndex := map[int][]model.ChainEvent{}
			for _, event := range events {
				if event.Phase == 0 {
					byIndex[event.ExtrinsicIdx] = append(byIndex[event.ExtrinsicIdx], event)
				}
			}
			for _, group := range byIndex {
				fee, err := model.ActualFeeFromEvents(group)
				require.NoError(t, err)
				if fee != nil {
					observedFees++
					for _, event := range group {
						if strings.EqualFold(event.ModuleId, "TransactionPayment") && strings.EqualFold(event.EventId, "TransactionFeePaid") {
							rawAmount, err := model.UnsignedAmount(event.Params[1].Value)
							require.NoError(t, err)
							require.True(t, rawAmount.Equal(*fee))
						}
					}
				}
			}
		}
	}
	require.Equal(t, 107, totalEvents)
	require.Equal(t, 20, observedFees)
}
