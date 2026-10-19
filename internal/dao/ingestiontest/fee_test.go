package ingestiontest

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/itering/subscan/model"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestFeeAvailabilityPersistence(t *testing.T) {
	d, _, _, _ := storageFixture(t)
	rows := []model.ChainExtrinsic{
		{ID: 4200000, BlockNum: 42, ExtrinsicIndex: "42-0", IsSigned: true, Fee: decimal.NewFromInt(999), UsedFee: decimal.NewFromInt(998)},                                                         // legacy: values are unverified
		{ID: 4200001, BlockNum: 42, ExtrinsicIndex: "42-1", IsSigned: true, FeeStatus: model.FeeEstimated, UsedFeeStatus: model.FeeAvailable, FeeStateHash: "parent42", UsedFeeStateHash: "hash42"}, // genuine zero
		{ID: 4200002, BlockNum: 42, ExtrinsicIndex: "42-2", IsSigned: true, FeeStatus: model.FeeUnknown, UsedFeeStatus: model.FeeAvailable, UsedFee: decimal.NewFromInt(100), FeeStateHash: "parent42", UsedFeeStateHash: "hash42", FeeError: "historical RPC unavailable"},
		{ID: 4200003, BlockNum: 42, ExtrinsicIndex: "42-3", FeeStatus: model.FeeNotApplicable, UsedFeeStatus: model.FeeNotApplicable},
	}
	ctx := context.Background()
	tx, err := d.DbBegin()
	require.NoError(t, err)
	defer d.DbRollback(tx)
	require.NoError(t, d.CreateExtrinsic(ctx, tx, rows, 3))
	require.NoError(t, d.CreateBlock(ctx, tx, &model.ChainBlock{ID: 42, BlockNum: 42, Hash: "hash42", Finalized: true, ExtrinsicsCount: 4}))
	require.NoError(t, d.DbCommit(tx))
	for index, input := range rows {
		stored := d.GetExtrinsicsByIndex(ctx, input.ExtrinsicIndex)
		require.NotNil(t, stored)
		// Exercise both DAO presenters used by the list and detail API endpoints.
		for _, result := range []interface{}{d.ExtrinsicsAsJson(stored), d.GetExtrinsicsDetailByIndex(ctx, input.ExtrinsicIndex)} {
			encoded, err := json.Marshal(result)
			require.NoError(t, err)
			var api map[string]interface{}
			require.NoError(t, json.Unmarshal(encoded, &api))
			switch index {
			case 0:
				require.Nil(t, api["fee"])
				require.Nil(t, api["used_fee"])
				require.Equal(t, model.FeeUnknown, api["fee_status"])
			case 1:
				require.Equal(t, "0", api["fee"])
				require.Equal(t, "0", api["used_fee"])
				require.Equal(t, model.FeeEstimated, api["fee_status"])
				require.Equal(t, model.FeeAvailable, api["used_fee_status"])
			case 2:
				require.Nil(t, api["fee"])
				require.Equal(t, "100", api["used_fee"])
				require.Equal(t, "parent42", api["fee_state_hash"])
				require.Equal(t, "hash42", api["used_fee_state_hash"])
				require.Equal(t, input.FeeError, api["fee_error"])
			case 3:
				require.Nil(t, api["fee"])
				require.Nil(t, api["used_fee"])
				require.Equal(t, model.FeeNotApplicable, api["fee_status"])
			}
		}
	}
}
