package ingestiontest

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/itering/subscan/model"
	"github.com/stretchr/testify/require"
)

func TestBlockStatePersistence(t *testing.T) {
	d, _, _, _ := storageFixture(t)
	zero := uint32(0)
	states := []model.BlockStateInfo{
		{}, // Legacy rows are unverified.
		{SessionIndex: &zero, SessionStatus: model.StateAvailable, AuthorStatus: model.StateAvailable, SessionStateHash: "hash43", AuthorStateHash: "hash43"},
		{SessionStatus: model.StateUnknown, AuthorStatus: model.StateUnknown, SessionStateHash: "hash44", AuthorStateHash: "hash44", SessionError: "historical state pruned", AuthorError: "historical state pruned"},
		{SessionStatus: model.StateAbsent, AuthorStatus: model.StateAbsent},
	}
	ctx := context.Background()
	for i, state := range states {
		height := uint(42 + i)
		tx, err := d.DbBegin()
		require.NoError(t, err)
		block := &model.ChainBlock{ID: height, BlockNum: height, Finalized: true, BlockStateInfo: state}
		require.NoError(t, d.CreateBlock(ctx, tx, block))
		require.NoError(t, d.DbCommit(tx))
		stored := d.GetBlockByNum(ctx, height)
		require.NotNil(t, stored)
		encoded, err := json.Marshal(d.BlockAsJson(ctx, stored))
		require.NoError(t, err)
		var api map[string]interface{}
		require.NoError(t, json.Unmarshal(encoded, &api))
		if i == 1 {
			require.Equal(t, float64(0), api["session_index"])
			require.Equal(t, model.StateAvailable, api["session_status"])
		} else {
			require.Nil(t, api["session_index"])
		}
		if i == 0 {
			require.Equal(t, model.StateUnknown, api["session_status"])
			require.Equal(t, model.StateUnknown, api["author_status"])
		}
		if i == 2 {
			require.Equal(t, state.SessionError, api["session_error"])
			require.Equal(t, state.AuthorError, api["author_error"])
			require.Equal(t, "hash44", api["author_state_hash"])
		}
		if i == 3 {
			require.Equal(t, model.StateAbsent, api["session_status"])
		}
	}
}
