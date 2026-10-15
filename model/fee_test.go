package model

import (
	"encoding/json"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestActualFeeAvailability(t *testing.T) {
	event := ChainEvent{Phase: 0, ModuleId: "TransactionPayment", EventId: "TransactionFeePaid", Params: EventParams{{Value: "account"}, {Value: "100"}, {Value: "7"}}}
	amount, err := ActualFeeFromEvents([]ChainEvent{event})
	require.NoError(t, err)
	require.Equal(t, "100", amount.String())
	amount, err = ActualFeeFromEvents(nil)
	require.NoError(t, err)
	require.Nil(t, amount)
	amount, err = ActualFeeFromEvents([]ChainEvent{event, event})
	require.Error(t, err)
	require.Nil(t, amount)
	for _, value := range []interface{}{nil, "", "-1", "1.5", float64(9007199254740992)} {
		_, err := UnsignedAmount(value)
		require.Error(t, err)
	}
	for _, value := range []interface{}{"0x20000000000001", json.Number("9007199254740993"), uint64(9007199254740993)} {
		amount, err := UnsignedAmount(value)
		require.NoError(t, err)
		require.Equal(t, "9007199254740993", amount.String())
	}
	event.Phase = 1
	amount, err = ActualFeeFromEvents([]ChainEvent{event})
	require.NoError(t, err)
	require.Nil(t, amount)
	info := (&ChainExtrinsic{Fee: decimal.NewFromInt(123), UsedFee: decimal.NewFromInt(100)}).FeeInfo()
	require.Nil(t, info.Fee)
	require.Nil(t, info.UsedFee)
	require.Equal(t, FeeUnknown, info.FeeStatus)
	require.Equal(t, FeeUnknown, info.UsedFeeStatus)
}
