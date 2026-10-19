package model

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/shopspring/decimal"
)

const (
	FeeUnknown       = "unknown"
	FeeEstimated     = "estimated"
	FeeAvailable     = "available"
	FeeNotApplicable = "not_applicable"
)

// FeeInfo is the API representation. A missing/unverified observation is null,
// including legacy rows that predate explicit availability tracking.
type FeeInfo struct {
	Fee              *decimal.Decimal `json:"fee"`
	UsedFee          *decimal.Decimal `json:"used_fee"`
	FeeStatus        string           `json:"fee_status"`
	UsedFeeStatus    string           `json:"used_fee_status"`
	FeeStateHash     string           `json:"fee_state_hash"`
	UsedFeeStateHash string           `json:"used_fee_state_hash"`
	FeeError         string           `json:"fee_error,omitempty"`
}

func (e *ChainExtrinsic) FeeInfo() FeeInfo {
	info := FeeInfo{FeeStatus: e.FeeStatus, UsedFeeStatus: e.UsedFeeStatus, FeeStateHash: e.FeeStateHash, UsedFeeStateHash: e.UsedFeeStateHash, FeeError: e.FeeError}
	if info.FeeStatus == "" {
		info.FeeStatus = FeeUnknown
	}
	if info.UsedFeeStatus == "" {
		info.UsedFeeStatus = FeeUnknown
	}
	if info.FeeStatus == FeeEstimated {
		value := e.Fee
		info.Fee = &value
	}
	if info.UsedFeeStatus == FeeAvailable {
		value := e.UsedFee
		info.UsedFee = &value
	}
	return info
}

// UnsignedAmount rejects missing, fractional and imprecise float inputs. Decimal
// strings/JSON integer tokens and SCALE integer values retain their exact value.
func UnsignedAmount(value interface{}) (decimal.Decimal, error) {
	var text string
	switch v := value.(type) {
	case string:
		text = v
	case json.Number:
		text = v.String()
	case uint64:
		text = fmt.Sprint(v)
	case uint:
		text = fmt.Sprint(v)
	case uint32:
		text = fmt.Sprint(v)
	case int:
		text = fmt.Sprint(v)
	case int64:
		text = fmt.Sprint(v)
	default:
		return decimal.Zero, fmt.Errorf("invalid amount type %T", value)
	}
	base := 10
	if strings.HasPrefix(text, "0x") {
		text = strings.TrimPrefix(text, "0x")
		base = 16
	}
	if text == "" {
		return decimal.Zero, fmt.Errorf("missing amount")
	}
	for _, ch := range text {
		if (ch < '0' || ch > '9') && (base != 16 || (ch < 'a' || ch > 'f') && (ch < 'A' || ch > 'F')) {
			return decimal.Zero, fmt.Errorf("invalid unsigned integer amount")
		}
	}
	number, ok := new(big.Int).SetString(text, base)
	if !ok || number.Sign() < 0 {
		return decimal.Zero, fmt.Errorf("invalid unsigned integer amount")
	}
	if len(number.String()) > 65 {
		return decimal.Zero, fmt.Errorf("amount exceeds decimal(65,0) storage")
	}
	return decimal.NewFromBigInt(number, 0), nil
}

// ActualFeeFromEvents preserves TransactionFeePaid.actual_fee in full, including
// its tip. Multiple/malformed records are unknown, never coerced to zero.
func ActualFeeFromEvents(events []ChainEvent) (*decimal.Decimal, error) {
	var fee *decimal.Decimal
	for _, event := range events {
		if event.Phase != 0 || !strings.EqualFold(event.ModuleId, "TransactionPayment") || !strings.EqualFold(event.EventId, "TransactionFeePaid") {
			continue
		}
		if fee != nil {
			return nil, fmt.Errorf("multiple TransactionFeePaid events")
		}
		if len(event.Params) < 3 {
			return nil, fmt.Errorf("incomplete TransactionFeePaid event")
		}
		amount, err := UnsignedAmount(event.Params[1].Value)
		if err != nil {
			return nil, err
		}
		tip, err := UnsignedAmount(event.Params[2].Value)
		if err != nil || tip.GreaterThan(amount) {
			return nil, fmt.Errorf("invalid TransactionFeePaid tip")
		}
		fee = &amount
	}
	return fee, nil
}
