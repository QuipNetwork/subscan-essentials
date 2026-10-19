package service

import (
	"context"
	"github.com/itering/subscan/model"
	"github.com/itering/subscan/util"
	"github.com/itering/subscan/util/mq"
	substrate "github.com/itering/substrate-api-rpc"
	"github.com/itering/substrate-api-rpc/metadata"
	rpcModel "github.com/itering/substrate-api-rpc/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestService_CreateChainBlock(t *testing.T) {
	hash := "0x2895f79f46105d24813c97558f06edaecf993fad2358dcaf86b813859edd697d"
	event := "0x080000000000000080e36a0900000000020000000100000000000000000000000000020000"
	block := rpcModel.Block{
		Extrinsics: []string{"0x280402000b10449a7e7301", "0x1c0407005e8b4100"},
		Header: rpcModel.ChainNewHeadResult{
			ExtrinsicsRoot: "0x5a9403235d77280ad129b44eebdbdea3127ada25dcbf540e6ce38a1f770ad86f",
			Number:         "0x1062da",
			ParentHash:     "0x42838e9a502c5ba1faa5de1f19bfe5464b3374df5c6f53152d0543363a900bb7",
			StateRoot:      "0x8c9eec7854ab7cc1207a3eb9d54e272a07545b2c77dd2b99f3b85a642cd91a49",
			Digest: rpcModel.ChainNewHeadLog{
				Logs: []string{
					"0x064241424534021b00000007b6d90f00000000",
					"0x00904d4d5252708a1db71fe9eedba2439dab7209846247482fe486dc5e82ef76897f2e50a3a5",
					"0x05424142450101aed0a28294d357326d3b199cd06f23a43cd44412cc9450286252bb75c47fad17a4e9a7e19fde1cb90d668b2bbc769faea28f63bffc53a28c8f5a9d817bf07b83",
				},
			},
		},
	}

	observedService := func(t *testing.T) (*Service, *blockIntegrityDao, *blockIntegrityQueue) {
		t.Helper()
		d := &blockIntegrityDao{
			extrinsics: map[uint]model.ChainExtrinsic{},
			events:     map[uint]model.ChainEvent{},
			blocks:     map[uint]model.ChainBlock{},
		}
		q := &blockIntegrityQueue{dao: d}
		previousQueue := mq.Instant
		mq.Instant = q
		t.Cleanup(func() { mq.Instant = previousQueue })
		return &Service{dao: d}, d, q
	}

	t.Run("reject incompatible legacy event fixture", func(t *testing.T) {
		// Preserve the original input as a negative regression. Its event
		// records encode eight-byte weights, but the default legacy registry
		// defines Weight as u32. Metadata V11 names DispatchInfo without
		// supplying a portable definition that could resolve that mismatch.
		// Previously CreateChainBlock logged the failure and returned success.
		m, metadataErr := testSrv.getMetadataInstant(4, "")
		require.NoError(t, metadataErr)
		require.Equal(t, 11, m.MetadataVersion)
		require.Equal(t, []string{"DispatchInfo"}, m.EventIndex["0000"].Call.Args)
		decoded, err := substrate.DecodeExtrinsic(block.Extrinsics, m, 4)
		require.NoError(t, err, "the original extrinsic body still decodes")
		require.Len(t, decoded, 2)
		decodedEvents, eventErr := substrate.DecodeEvent(event, m, 4)
		if eventErr == nil {
			_, _, eventErr = convertBlockData(block.Extrinsics, event, decoded, decodedEvents)
		}
		require.Error(t, eventErr, "the original event/registry pairing must fail")

		s, d, q := observedService(t)
		err = s.CreateChainBlock(context.Background(), hash, &block, event, 4, 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "block 1073882")
		require.Empty(t, d.actions, "no transaction, cache, writes or progress")
		require.False(t, d.committed)
		require.Empty(t, d.blocks)
		require.Empty(t, d.extrinsics)
		require.Empty(t, d.events)
		require.Zero(t, q.calls)
	})

	t.Run("valid metadata-paired block", func(t *testing.T) {
		// Synthetic input with an explicit matching metadata definition;
		// no fee/author RPC, database server or public chain is involved.
		validBlock, validEvents, m := integrityInputs(t)
		const spec = 900002
		previous := metadata.RuntimeMetadata[spec]
		metadata.RuntimeMetadata[spec] = m
		t.Cleanup(func() {
			if previous == nil {
				delete(metadata.RuntimeMetadata, spec)
			} else {
				metadata.RuntimeMetadata[spec] = previous
			}
		})
		s, d, q := observedService(t)
		require.NoError(t, s.CreateChainBlock(context.Background(), "synthetic-block", validBlock, validEvents, spec, 0))
		require.True(t, d.committed)
		require.Len(t, d.blocks, 1)
		require.Len(t, d.extrinsics, 3)
		require.Len(t, d.events, 3)
		require.Equal(t, 3, d.blocks[42].ExtrinsicsCount)
		require.Equal(t, 3, d.blocks[42].EventCount)
		require.False(t, d.blocks[42].CodecError)
		require.Positive(t, q.calls, "notifications are emitted after commit")
	})

}

func TestService_checkoutExtrinsicEvents(t *testing.T) {
	event1 := model.ChainEvent{BlockNum: 107388, ExtrinsicIdx: 1}
	event2 := model.ChainEvent{BlockNum: 107388, ExtrinsicIdx: 2}
	events := testSrv.checkoutExtrinsicEvents([]model.ChainEvent{event1, event2}, 107388)
	assert.Equal(t, map[string][]model.ChainEvent{"107388-1": {event1}, "107388-2": {event2}}, events)
}

func TestService_GetCurrentRuntimeSpecVersion(t *testing.T) {
	assert.Equal(t, 4, testSrv.GetCurrentRuntimeSpecVersion(107388))
}

func TestService_GetExtrinsicList(t *testing.T) {
	_, page := testSrv.GetExtrinsicList(context.Background(), 10, -1, 0, 0, "")
	assert.Equal(t, false, page.HasPreviousPage)
}

func TestService_GetBlocksSampleByNums(t *testing.T) {
	util.AddressType = "42"
	blocks, _ := testSrv.GetBlocksSampleCursor(context.TODO(), 10, 0, 0)
	assert.Equal(t, []model.SampleBlockJson{{
		BlockStateInfo: model.BlockStateInfo{SessionStatus: model.StateUnknown, AuthorStatus: model.StateUnknown},
		BlockNum:       947687,
		BlockTimestamp: 1594791900,
		Hash:           "0xd68b38c412404a4b5d4974e6dbb4a491ed7b6200d4edc24152693804441ce99d",
		Validator:      "5EFjtKj1r8kEvDSeDAMaAJxciDt2G33n7hPHjDDRiYPZNWCD",
		Finalized:      true,
	}}, blocks)
}
