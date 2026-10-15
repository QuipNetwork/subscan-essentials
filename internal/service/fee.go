package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"

	core "github.com/itering/subscan/model"
	"github.com/itering/subscan/util"
	rpcmodel "github.com/itering/substrate-api-rpc/model"
	"github.com/itering/substrate-api-rpc/rpc"
	"github.com/itering/substrate-api-rpc/websocket"
	"github.com/shopspring/decimal"
)

type PaymentQueryFeeDetails struct {
	InclusionFee *inclusionFee `json:"inclusionFee"`
}
type inclusionFee struct{ BaseFee, LenFee, AdjustedWeightFee decimal.Decimal }

func (p *PaymentQueryFeeDetails) EstimateFee() decimal.Decimal {
	if p.InclusionFee == nil {
		return decimal.Zero
	}
	return p.InclusionFee.BaseFee.Add(p.InclusionFee.LenFee).Add(p.InclusionFee.AdjustedWeightFee)
}
func amountJSON(raw json.RawMessage) (decimal.Decimal, error) {
	var value interface{}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return decimal.Zero, err
	}
	return core.UnsignedAmount(value)
}
func (p *PaymentQueryFeeDetails) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	raw, ok := fields["inclusionFee"]
	if !ok {
		return fmt.Errorf("missing inclusionFee")
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		p.InclusionFee = nil
		return nil
	}
	var parts map[string]json.RawMessage
	if err := json.Unmarshal(raw, &parts); err != nil {
		return err
	}
	fee := &inclusionFee{}
	for name, out := range map[string]*decimal.Decimal{"baseFee": &fee.BaseFee, "lenFee": &fee.LenFee, "adjustedWeightFee": &fee.AdjustedWeightFee} {
		value, err := amountJSON(parts[name])
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		*out = value
	}
	p.InclusionFee = fee
	return nil
}

type PaymentQueryInfo struct {
	Class      string
	PartialFee decimal.Decimal
	Weight     decimal.Decimal
	ProofSize  uint64
}

func (p *PaymentQueryInfo) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	fee, err := amountJSON(fields["partialFee"])
	if err != nil {
		return fmt.Errorf("partialFee: %w", err)
	}
	p.PartialFee = fee
	if err := json.Unmarshal(fields["class"], &p.Class); err != nil {
		return err
	}
	weight := bytes.TrimSpace(fields["weight"])
	if len(weight) > 0 && weight[0] == '{' {
		var parts map[string]json.RawMessage
		if err := json.Unmarshal(weight, &parts); err != nil {
			return err
		}
		ref := parts["ref_time"]
		if ref == nil {
			ref = parts["refTime"]
		}
		proof := parts["proof_size"]
		if proof == nil {
			proof = parts["proofSize"]
		}
		p.Weight, err = amountJSON(ref)
		if err != nil {
			return err
		}
		proofValue, err := amountJSON(proof)
		if err != nil {
			return err
		}
		if !proofValue.BigInt().IsUint64() || !p.Weight.BigInt().IsUint64() {
			return fmt.Errorf("WeightV2 exceeds uint64")
		}
		p.ProofSize = proofValue.BigInt().Uint64()
	} else {
		p.Weight, err = amountJSON(weight)
		if err != nil {
			return err
		}
		if !p.Weight.BigInt().IsUint64() {
			return fmt.Errorf("weight exceeds uint64")
		}
	}
	return nil
}

func SystemPaymentQueryInfo(id int, encodedExtrinsic, hash string) []byte {
	p := rpc.Param{Id: id, JsonRpc: "2.0", Method: "payment_queryInfo", Params: []string{util.AddHex(encodedExtrinsic), hash}}
	b, _ := json.Marshal(p)
	return b
}
func SystemPaymentQueryFeeDetails(id int, encodedExtrinsic, hash string) []byte {
	p := rpc.Param{Id: id, JsonRpc: "2.0", Method: "payment_queryFeeDetails", Params: []string{util.AddHex(encodedExtrinsic), hash}}
	b, _ := json.Marshal(p)
	return b
}

// Keep result bytes intact until decoding typed amounts/weights. JsonRpcResult's
// interface{} decoding would round large JSON integers through float64.
func paymentRPC(ctx context.Context, hash string, request []byte, out interface{}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if hash == "" {
		return fmt.Errorf("fee query requires an explicit state hash")
	}
	var response struct {
		Result json.RawMessage `json:"result"`
		Error  *rpcmodel.Error `json:"error"`
	}
	if err := websocket.SendWsRequest(nil, &response, request); err != nil {
		return err
	}
	if response.Error != nil {
		return fmt.Errorf("fee RPC %d: %s", response.Error.Code, response.Error.Message)
	}
	if len(response.Result) == 0 || bytes.Equal(bytes.TrimSpace(response.Result), []byte("null")) {
		return fmt.Errorf("missing fee RPC result")
	}
	return json.Unmarshal(response.Result, out)
}
func GetPaymentQueryInfo(ctx context.Context, _ int, encodedExtrinsic, hash string, _ bool) (*PaymentQueryInfo, error) {
	result := &PaymentQueryInfo{}
	if err := paymentRPC(ctx, hash, SystemPaymentQueryInfo(rand.Intn(10000), encodedExtrinsic, hash), result); err != nil {
		return nil, err
	}
	return result, nil
}
func GetPaymentQueryFeeDetails(ctx context.Context, encodedExtrinsic, hash string) (*PaymentQueryFeeDetails, error) {
	result := &PaymentQueryFeeDetails{}
	if err := paymentRPC(ctx, hash, SystemPaymentQueryFeeDetails(rand.Intn(10000), encodedExtrinsic, hash), result); err != nil {
		return nil, err
	}
	return result, nil
}
func estimateExtrinsicFee(ctx context.Context, encodedExtrinsic, hash string) (decimal.Decimal, error) {
	details, detailsErr := GetPaymentQueryFeeDetails(ctx, encodedExtrinsic, hash)
	if detailsErr == nil {
		fee := details.EstimateFee()
		if len(fee.String()) <= 65 {
			return fee, nil
		}
		detailsErr = fmt.Errorf("fee estimate exceeds decimal(65,0) storage")
	}
	info, infoErr := GetPaymentQueryInfo(ctx, 0, encodedExtrinsic, hash, false)
	if infoErr == nil {
		return info.PartialFee, nil
	}
	return decimal.Zero, errors.Join(detailsErr, infoErr)
}

// GetExtrinsicFee retains the legacy tuple helper. Core ingestion uses explicit
// availability fields below. Actual fee is only from an event, never inferred
// by scaling a fee with one component of WeightV2.
func GetExtrinsicFee(ctx context.Context, raw, hash string, _ int, _, eventFee decimal.Decimal, _ bool) (decimal.Decimal, decimal.Decimal, error) {
	actual := decimal.Zero
	if !eventFee.IsNegative() {
		actual = eventFee
	}
	fee, err := estimateExtrinsicFee(ctx, raw, hash)
	if eventFee.IsNegative() {
		err = errors.Join(err, fmt.Errorf("actual fee unavailable"))
	}
	return fee, actual, err
}

func enrichExtrinsicFee(ctx context.Context, e *core.ChainExtrinsic, raw string, block *core.ChainBlock, events []core.ChainEvent) {
	e.Fee, e.UsedFee = decimal.Zero, decimal.Zero
	e.FeeStateHash, e.UsedFeeStateHash, e.FeeError = "", "", ""
	e.FeeStatus, e.UsedFeeStatus = core.FeeUnknown, core.FeeUnknown
	eventFee, eventErr := core.ActualFeeFromEvents(events)
	if eventFee != nil {
		e.UsedFee = *eventFee
		e.UsedFeeStatus = core.FeeAvailable
		e.UsedFeeStateHash = block.Hash
	}
	version, versionErr := extrinsicWireVersion(raw)
	if versionErr != nil {
		e.FeeError = versionErr.Error()
		return
	}
	if version == 0x04 || version == 0x05 {
		e.FeeStatus = core.FeeNotApplicable
		if eventFee == nil && eventErr == nil {
			e.UsedFeeStatus = core.FeeNotApplicable
		}
		if eventErr != nil {
			e.FeeError = eventErr.Error()
		}
		return
	}
	// General V5 extrinsics are not signed, but may still pay fees.
	e.FeeStateHash = block.ParentHash
	fee, estimateErr := estimateExtrinsicFee(ctx, raw, block.ParentHash)
	if estimateErr == nil {
		e.Fee = fee
		e.FeeStatus = core.FeeEstimated
	}
	if err := errors.Join(eventErr, estimateErr); err != nil {
		e.FeeError = err.Error()
	}
}
