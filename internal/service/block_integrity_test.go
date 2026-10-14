package service

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/itering/scale.go/types"
	"github.com/itering/subscan/internal/dao"
	"github.com/itering/subscan/model"
	"github.com/itering/subscan/util/mq"
	substrate "github.com/itering/substrate-api-rpc"
	"github.com/itering/substrate-api-rpc/metadata"
	smodel "github.com/itering/substrate-api-rpc/model"
	"github.com/stretchr/testify/require"
)

type blockIntegrityDao struct {
	MockDao
	actions    []string
	extrinsics map[uint]model.ChainExtrinsic
	events     map[uint]model.ChainEvent
	blocks     map[uint]model.ChainBlock
	committed  bool
}

func (d *blockIntegrityDao) SplitBlockTable(uint) { d.actions = append(d.actions, "split/cache") }
func (d *blockIntegrityDao) DbBegin() *dao.GormDB {
	d.actions = append(d.actions, "begin")
	return &dao.GormDB{}
}
func (d *blockIntegrityDao) DbCommit(*dao.GormDB) {
	d.actions = append(d.actions, "commit")
	d.committed = true
}
func (d *blockIntegrityDao) DbRollback(*dao.GormDB) { d.actions = append(d.actions, "rollback") }
func (d *blockIntegrityDao) CreateExtrinsic(_ context.Context, _ *dao.GormDB, rows []model.ChainExtrinsic, _ int) error {
	d.actions = append(d.actions, "extrinsics")
	for _, row := range rows {
		d.extrinsics[row.ID] = row
	}
	return nil
}
func (d *blockIntegrityDao) CreateEvent(_ *dao.GormDB, rows []model.ChainEvent) error {
	d.actions = append(d.actions, "events")
	for _, row := range rows {
		d.events[row.ID] = row
	}
	return nil
}
func (d *blockIntegrityDao) CreateBlock(_ context.Context, _ *dao.GormDB, row *model.ChainBlock) error {
	d.actions = append(d.actions, "block")
	d.blocks[row.ID] = *row
	return nil
}
func (d *blockIntegrityDao) SaveFillAlreadyBlockNum(context.Context, int) error {
	d.actions = append(d.actions, "progress")
	return nil
}
func (d *blockIntegrityDao) SaveFillAlreadyFinalizedBlockNum(context.Context, int) error {
	d.actions = append(d.actions, "finalized")
	return nil
}

type blockIntegrityQueue struct {
	mq.IJob
	calls int
	dao   *blockIntegrityDao
}

func (q *blockIntegrityQueue) Publish(string, string, interface{}) error {
	if !q.dao.committed {
		return fmt.Errorf("notification before commit")
	}
	q.calls++
	return nil
}

func integrityInputs(t *testing.T) (*smodel.Block, string, *metadata.Instant) {
	t.Helper()
	m := &metadata.Instant{MetadataVersion: 13,
		CallIndex: map[string]types.CallIndex{"0000": {
			Module: types.MetadataModules{Name: "Example"},
			Call:   types.MetadataCalls{Name: "set_value", Args: []types.MetadataModuleCallArgument{{Name: "value", Type: "U32"}}},
		}},
		EventIndex: map[string]types.EventIndex{"0000": {
			Module: types.MetadataModules{Name: "Example"}, Call: types.MetadataEvents{Name: "Ready"},
		}},
	}
	block := &smodel.Block{Header: smodel.ChainNewHeadResult{Number: "0x2a"}}
	events := []byte{12} // Vec length 3
	for index := 0; index < 3; index++ {
		raw := []byte{28, 4, 0, 0, 0, 0, 0, 0}
		if index == 0 {
			raw[1] = 5 // current bare V5 alongside legacy bare V4
		}
		binary.LittleEndian.PutUint32(raw[4:], uint32(index+11))
		block.Extrinsics = append(block.Extrinsics, "0x"+hex.EncodeToString(raw))
		event := make([]byte, 8) // ApplyExtrinsic(index), event 0000, empty topics
		binary.LittleEndian.PutUint32(event[1:], uint32(index))
		events = append(events, event...)
	}
	return block, "0x" + hex.EncodeToString(events), m
}

func TestBlockDecodeFailureHasNoEffects(t *testing.T) {
	const spec = 900001
	_, _, m := integrityInputs(t)
	previous := metadata.RuntimeMetadata[spec]
	metadata.RuntimeMetadata[spec] = m
	t.Cleanup(func() {
		if previous == nil {
			delete(metadata.RuntimeMetadata, spec)
		} else {
			metadata.RuntimeMetadata[spec] = previous
		}
	})
	for _, failure := range []string{"first", "middle", "last", "event", "digest"} {
		t.Run(failure, func(t *testing.T) {
			block, events, _ := integrityInputs(t)
			d := &blockIntegrityDao{extrinsics: map[uint]model.ChainExtrinsic{}, events: map[uint]model.ChainEvent{}, blocks: map[uint]model.ChainBlock{}}
			s := &Service{dao: d}
			q := &blockIntegrityQueue{dao: d}
			previousQueue := mq.Instant
			mq.Instant = q
			t.Cleanup(func() { mq.Instant = previousQueue })
			switch failure {
			case "first", "middle", "last":
				index := map[string]int{"first": 0, "middle": 1, "last": 2}[failure]
				block.Extrinsics[index] = "0x0c04ffff"
			case "event":
				events = "0x040000000000ffff00"
			case "digest":
				block.Header.Digest.Logs = []string{"0xff"}
			}
			err := s.CreateChainBlock(context.Background(), "hash", block, events, spec, 0)
			require.Error(t, err)
			require.Empty(t, d.actions, "no transaction, cache, persistence or progress on decode failure")
			require.Zero(t, q.calls)
			require.Empty(t, d.blocks)
			require.Empty(t, d.extrinsics)
			require.Empty(t, d.events)
			// Corrected retry reaches commit and notifications in that order.
			block, events, _ = integrityInputs(t)
			require.NoError(t, s.CreateChainBlock(context.Background(), "hash", block, events, spec, 0))
			require.True(t, d.committed)
			require.Positive(t, q.calls)
			require.Len(t, d.blocks, 1)
			require.Len(t, d.extrinsics, 3)
			require.Len(t, d.events, 3)
			for index := 0; index < 3; index++ {
				id := uint(42*model.IdGenerateCoefficient + index)
				require.Equal(t, fmt.Sprintf("42-%d", index), d.extrinsics[id].ExtrinsicIndex)
				require.False(t, d.extrinsics[id].IsSigned)
				require.Len(t, d.extrinsics[id].ExtrinsicHash, 66)
				require.Equal(t, fmt.Sprintf("42-%d", index), d.events[id].ExtrinsicIndex)
			}
		})
	}
}

func TestConvertBlockDataRejectsPartialOrInvalidResults(t *testing.T) {
	block, rawEvents, m := integrityInputs(t)
	decoded, err := substrate.DecodeExtrinsic(block.Extrinsics, m, 1)
	require.NoError(t, err)
	decodedEvents, err := substrate.DecodeEvent(rawEvents, m, 1)
	require.NoError(t, err)
	var validEvents []model.ChainEvent
	encodedEvents, err := json.Marshal(decodedEvents)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(encodedEvents, &validEvents))
	for _, test := range []struct {
		name       string
		extrinsics []map[string]interface{}
		events     interface{}
		cause      string
	}{
		{"partial extrinsics", decoded[:2], decodedEvents, "extrinsic count mismatch"},
		{"extra extrinsics", append(append([]map[string]interface{}{}, decoded...), decoded[0]), decodedEvents, "extrinsic count mismatch"},
		{"conversion", []map[string]interface{}{{"nonce": "invalid"}, decoded[1], decoded[2]}, decodedEvents, "convert extrinsics"},
		{"nil extrinsic", []map[string]interface{}{nil, decoded[1], decoded[2]}, decodedEvents, "missing decoded call"},
		{"event conversion", decoded, "invalid json", "convert events"},
		{"partial events", decoded, []model.ChainEvent{}, "event count mismatch"},
		{"nil events", decoded, nil, "event count mismatch"},
	} {
		t.Run(test.name, func(t *testing.T) {
			extrinsics, events, err := convertBlockData(block.Extrinsics, rawEvents, test.extrinsics, test.events)
			require.ErrorContains(t, err, test.cause)
			require.Nil(t, extrinsics)
			require.Nil(t, events)
		})
	}
	for _, test := range []struct {
		name   string
		change func(*model.ChainEvent)
	}{
		{"event order", func(e *model.ChainEvent) { e.EventIdx = 2 }},
		{"out of range extrinsic", func(e *model.ChainEvent) { e.ExtrinsicIdx = 3 }},
		{"negative extrinsic", func(e *model.ChainEvent) { e.ExtrinsicIdx = -1 }},
		{"invalid phase", func(e *model.ChainEvent) { e.Phase = 3 }},
		{"missing event", func(e *model.ChainEvent) { e.EventId = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalidEvents := append([]model.ChainEvent{}, validEvents...)
			test.change(&invalidEvents[0])
			extrinsics, events, err := convertBlockData(block.Extrinsics, rawEvents, decoded, invalidEvents)
			require.Error(t, err)
			require.Nil(t, extrinsics)
			require.Nil(t, events)
		})
	}
}

func TestBlockSignednessUsesWire(t *testing.T) {
	for _, test := range []struct {
		version   byte
		signature string
		signed    bool
	}{
		{0x84, "", true}, {0x84, "composite", true}, {0x85, "", true},
		{0x04, "misleading", false}, {0x05, "", false}, {0x45, "authorization", false},
	} {
		t.Run(fmt.Sprintf("%02x/%s", test.version, test.signature), func(t *testing.T) {
			raw := fmt.Sprintf("0x0c%02x0000", test.version)
			decoded := []map[string]interface{}{{"call_module": "Example", "call_module_function": "call", "signature": test.signature}}
			extrinsics, _, err := convertBlockData([]string{raw}, "0x00", decoded, nil)
			require.NoError(t, err)
			require.Equal(t, test.signed, extrinsics[0].IsSigned)
		})
	}
}

func TestBlockEventPhaseAssociation(t *testing.T) {
	d := &blockIntegrityDao{events: map[uint]model.ChainEvent{}}
	s := &Service{dao: d}
	events := []model.ChainEvent{
		{EventIdx: 0, Phase: 0, ExtrinsicIdx: 1},
		{EventIdx: 1, Phase: 1, ExtrinsicIdx: 0},
		{EventIdx: 2, Phase: 2, ExtrinsicIdx: 0},
	}
	associated := s.checkoutExtrinsicEvents(events, 42)
	require.Len(t, associated, 1)
	require.Equal(t, []model.ChainEvent{events[0]}, associated["42-1"])
	require.NoError(t, s.AddEvent(&dao.GormDB{}, &model.ChainBlock{BlockNum: 42}, events))
	require.Equal(t, "42-1", d.events[4200000].ExtrinsicIndex)
	require.Empty(t, d.events[4200001].ExtrinsicIndex)
	require.Empty(t, d.events[4200002].ExtrinsicIndex)
	require.Equal(t, 1, d.events[4200001].Phase)
	require.Equal(t, 2, d.events[4200002].Phase)
}

func TestBlockInvalidWireHeaders(t *testing.T) {
	for _, raw := range []string{"", "0x", "0x01", "0x0c", "0x0c04", "0x04040000", "0x0c060000", "0x0c04zz00"} {
		t.Run(raw, func(t *testing.T) {
			_, err := extrinsicWireVersion(raw)
			require.Error(t, err)
		})
	}
	for _, raw := range []string{"", "0x", "0xzz"} {
		_, _, err := convertBlockData(nil, raw, nil, nil)
		require.Error(t, err)
	}
}
