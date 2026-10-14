package service

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/itering/scale.go/types"
	"github.com/itering/subscan/model"
	"github.com/itering/subscan/util/mq"
	"github.com/itering/substrate-api-rpc/metadata"
	"github.com/stretchr/testify/require"
)

func TestExtrinsicOutcome(t *testing.T) {
	success := model.ChainEvent{ModuleId: "System", EventId: "ExtrinsicSuccess"}
	failure := model.ChainEvent{ModuleId: "System", EventId: "ExtrinsicFailed"}
	other := model.ChainEvent{ModuleId: "System", EventId: "NewAccount"}
	for _, test := range []struct {
		name   string
		events []model.ChainEvent
		want   bool
	}{
		{"success", []model.ChainEvent{success}, true},
		{"failure", []model.ChainEvent{failure}, false},
		{"no events", nil, false},
		{"missing outcome", []model.ChainEvent{other}, false},
		{"unrelated event before success", []model.ChainEvent{other, success}, true},
		{"unrelated event after success", []model.ChainEvent{success, other}, true},
		{"case insensitive", []model.ChainEvent{{ModuleId: "sYsTeM", EventId: "eXtRiNsIcSuCcEsS"}}, true},
		{"wrong module", []model.ChainEvent{{ModuleId: "Example", EventId: "ExtrinsicSuccess"}}, false},
		{"initialization is not an extrinsic outcome", []model.ChainEvent{{ModuleId: "System", EventId: "ExtrinsicSuccess", Phase: 2}}, false},
		{"finalization is not an extrinsic outcome", []model.ChainEvent{{ModuleId: "System", EventId: "ExtrinsicSuccess", Phase: 1}}, false},
		{"success then failure is ambiguous", []model.ChainEvent{success, failure}, false},
		{"failure then success is ambiguous", []model.ChainEvent{failure, success}, false},
		{"duplicate success is ambiguous", []model.ChainEvent{success, success}, false},
		{"duplicate failure is ambiguous", []model.ChainEvent{failure, failure}, false},
	} {
		t.Run(test.name, func(t *testing.T) { require.Equal(t, test.want, (&Service{}).getExtrinsicSuccess(test.events)) })
	}
}

func TestBlockPersistsExtrinsicOutcomes(t *testing.T) {
	const spec = 900004
	block, _, m := integrityInputs(t)
	for key, name := range map[string]string{"0001": "ExtrinsicSuccess", "0002": "ExtrinsicFailed"} {
		m.EventIndex[key] = types.EventIndex{Module: types.MetadataModules{Name: "System"}, Call: types.MetadataEvents{Name: name}}
	}
	previous := metadata.RuntimeMetadata[spec]
	metadata.RuntimeMetadata[spec] = m
	t.Cleanup(func() {
		if previous == nil {
			delete(metadata.RuntimeMetadata, spec)
		} else {
			metadata.RuntimeMetadata[spec] = previous
		}
	})
	// Deliberately interleave extrinsic 1's failure before extrinsic 0's success.
	// Extrinsic 2 has no outcome. The finalization success must not leak to 0/2.
	encoded := []byte{16} // four events
	for _, entry := range []struct {
		index uint32
		call  byte
	}{{1, 2}, {0, 1}, {2, 0}} {
		e := make([]byte, 8)
		binary.LittleEndian.PutUint32(e[1:], entry.index)
		e[6] = entry.call
		encoded = append(encoded, e...)
	}
	encoded = append(encoded, 1, 0, 1, 0) // Finalization, System.ExtrinsicSuccess, empty topics
	d := &blockIntegrityDao{blocks: map[uint]model.ChainBlock{}, extrinsics: map[uint]model.ChainExtrinsic{}, events: map[uint]model.ChainEvent{}}
	queue := mq.Instant
	mq.Instant = &blockIntegrityQueue{dao: d}
	t.Cleanup(func() { mq.Instant = queue })
	require.NoError(t, (&Service{dao: d}).CreateChainBlock(context.Background(), "hash", block, "0x"+hex.EncodeToString(encoded), spec, 0))
	require.True(t, d.committed)
	require.Len(t, d.extrinsics, 3)
	require.True(t, d.extrinsics[4200000].Success)
	require.False(t, d.extrinsics[4200001].Success, "explicit dispatch failure must survive persistence")
	require.False(t, d.extrinsics[4200002].Success, "missing dispatch outcome must not imply success")
	require.Equal(t, "42-1", d.events[4200000].ExtrinsicIndex)
	require.Equal(t, "42-0", d.events[4200001].ExtrinsicIndex)
	require.Empty(t, d.events[4200003].ExtrinsicIndex)
}
