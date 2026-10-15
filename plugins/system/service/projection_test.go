package service

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/itering/scale.go/types"
	"github.com/itering/scale.go/types/scaleBytes"
	"github.com/itering/subscan-plugin/storage"
	"github.com/itering/subscan/plugins/system/model"
	"github.com/itering/subscan/share/state"
	"github.com/itering/subscan/util"
	substrate "github.com/itering/substrate-api-rpc"
	"github.com/itering/substrate-api-rpc/metadata"
	"github.com/stretchr/testify/require"
)

type projectionStorage struct {
	storage.Dao
	err error
	row *model.ExtrinsicError
	raw string
}

func TestNativeDispatchErrorVariants(t *testing.T) {
	for _, value := range []interface{}{
		map[string]interface{}{"Arithmetic": "NotAnError"},
		map[string]interface{}{"Arithmetic": nil},
		map[string]interface{}{"Arithmetic": 0},
		map[string]interface{}{"Arithmetic": map[string]interface{}{"Underflow": nil}},
		map[string]interface{}{"Arithmetic": "Underflow", "Token": "Frozen"},
		map[string]interface{}{"UnknownCategory": "Underflow"},
	} {
		d := new(projectionStorage)
		require.Error(t, New(d).ExtrinsicFailed(119, &storage.Event{}, []storage.EventParam{{Type: "sp_runtime:DispatchError", Value: value}}))
		require.Nil(t, d.row)
	}
	root := os.Getenv("QUIP_CONFORMANCE_FIXTURES")
	if root == "" {
		t.Skip("requires QUIP_CONFORMANCE_FIXTURES")
	}
	raw, err := os.ReadFile(filepath.Join(root, "local-119", "metadata-107.scale"))
	require.NoError(t, err)
	require.Equal(t, "c37bbc6fd08ed2974dc1fb5ce7ae5bd28758e96df3bdef43c5da44ebcd2bc774", fmt.Sprintf("%x", sha256.Sum256(raw)))
	md, err := state.RegisterCheckedMetadata(&metadata.RuntimeRaw{Spec: 119, Raw: "0x" + hex.EncodeToString(raw)})
	require.NoError(t, err)
	for _, tc := range []struct {
		bytes []byte
		name  string
	}{
		{[]byte{0}, "Other"}, {[]byte{1}, "CannotLookup"}, {[]byte{2}, "BadOrigin"},
		{[]byte{4}, "ConsumerRemaining"}, {[]byte{5}, "NoProviders"}, {[]byte{6}, "TooManyConsumers"},
		{[]byte{10}, "Exhausted"}, {[]byte{11}, "Corruption"}, {[]byte{12}, "Unavailable"}, {[]byte{13}, "RootNotAllowed"},
		{[]byte{8, 0}, "Arithmetic.Underflow"}, {[]byte{8, 1}, "Arithmetic.Overflow"}, {[]byte{8, 2}, "Arithmetic.DivisionByZero"},
		{[]byte{7, 0}, "Token.FundsUnavailable"}, {[]byte{7, 5}, "Token.Frozen"}, {[]byte{7, 9}, "Token.Blocked"},
		{[]byte{9, 0}, "Transactional.LimitReached"}, {[]byte{9, 1}, "Transactional.NoLayer"},
		{[]byte{14, 4}, "Trie.InvalidHash"}, {[]byte{14, 13}, "Trie.DecodeError"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decoder := types.ScaleDecoder{}
			decoder.Init(scaleBytes.ScaleBytes{Data: tc.bytes}, &types.ScaleDecoderOption{Spec: 119, Metadata: (*types.MetadataStruct)(md)})
			value := decoder.ProcessAndUpdateData("sp_runtime:DispatchError")
			require.Zero(t, decoder.Data.GetRemainingLength())
			d := new(projectionStorage)
			e := &storage.Event{BlockNum: 15, ExtrinsicIdx: 1}
			params := []storage.EventParam{{Type: "sp_runtime:DispatchError", Value: value}}
			require.NoError(t, New(d).ExtrinsicFailed(119, e, params), "%#v", value)
			require.Equal(t, tc.name, d.row.Name)
			require.Empty(t, d.row.Module, "non-module errors must not invent a pallet")
			d.err = errors.New("injected persistence failure")
			require.ErrorIs(t, New(d).ExtrinsicFailed(119, e, params), d.err)
		})
	}
	// Actual failed transfer from a fresh ordinary node; independent polkadot-js
	// decoding reports Arithmetic.Underflow. Provenance accompanies these bytes.
	eventsRaw, err := os.ReadFile("../../../testdata/quip-dispatch-error/events-15.scale")
	require.NoError(t, err)
	require.Equal(t, "604935c9bda54dc5b1817c7ccd7aeec94bc34395e1a8637e8d93fc87f75b304d", fmt.Sprintf("%x", sha256.Sum256(eventsRaw)))
	decoded, err := substrate.DecodeEvent("0x"+hex.EncodeToString(eventsRaw), md, 119)
	require.NoError(t, err)
	var events []struct {
		Name   string               `json:"event_id"`
		Index  int                  `json:"extrinsic_idx"`
		Params []storage.EventParam `json:"params"`
	}
	require.NoError(t, util.UnmarshalAny(&events, decoded))
	require.Len(t, events, 11)
	failures := 0
	for _, event := range events {
		if event.Name != "ExtrinsicFailed" {
			continue
		}
		d := new(projectionStorage)
		require.Equal(t, 1, event.Index)
		require.NoError(t, New(d).ExtrinsicFailed(119, &storage.Event{BlockNum: 15, ExtrinsicIdx: event.Index}, event.Params))
		require.Equal(t, "Arithmetic.Underflow", d.row.Name)
		require.Empty(t, d.row.Module)
		failures++
	}
	require.Equal(t, 1, failures)
}

func (d *projectionStorage) SpecialMetadata(int) string { return d.raw }

func (d *projectionStorage) Create(row interface{}) error {
	if d.err != nil {
		return d.err
	}
	d.row = row.(*model.ExtrinsicError)
	return nil
}

func TestDispatchErrorFormats(t *testing.T) {
	for _, value := range []string{
		`{"Module":{"index":13,"error":"0x13000000"}}`,
		`{"Module":{"index":13,"error":"0x13abcdef"}}`,
		`{"Module":{"index":13,"error":19}}`,
		`{"Module":13,"Error":19}`,
	} {
		m, e, name, err := dispatchError(json.RawMessage(value))
		require.NoError(t, err, value)
		require.Equal(t, 13, m)
		require.Equal(t, 19, e)
		require.Empty(t, name)
	}
	for _, value := range []string{
		`null`, `{}`, `{"Module":null}`, `{"Module":{}}`,
		`{"Module":{"index":13}}`, `{"Module":{"error":"0x13000000"}}`,
		`{"Module":{"index":null,"error":19}}`, `{"Module":{"index":-1,"error":19}}`,
		`{"Module":{"index":256,"error":19}}`, `{"Module":{"index":13,"error":null}}`,
		`{"Module":{"index":13,"error":1.5}}`, `{"Module":{"index":13,"error":256}}`,
		`{"Module":{"index":13,"error":"0x13"}}`, `{"Module":{"index":13,"error":"0x1300000000"}}`,
		`{"Module":{"index":13,"error":"0xgg000000"}}`, `{"Error":19}`, `{"Module":13,"Error":-1}`,
		`{"BadOrigin":0}`, `{"BadOrigin":null,"Other":null}`, `{"Unsupported":null}`,
	} {
		_, _, _, err := dispatchError(json.RawMessage(value))
		require.Error(t, err, value)
	}
	for _, name := range []string{"Other", "CannotLookup", "BadOrigin"} {
		for _, value := range []interface{}{name, map[string]interface{}{name: nil}} {
			d := new(projectionStorage)
			err := New(d).ExtrinsicFailed(1, &storage.Event{BlockNum: 42, ExtrinsicIdx: 1}, []storage.EventParam{{Type: "sp_runtime:DispatchError", Value: value}})
			require.NoError(t, err)
			require.Equal(t, name, d.row.Name)
		}
	}
}

// This feeds native V16-decoded events through the same storage.EventParam
// serialization boundary as the durable System projection worker.
func TestArchivedDispatchErrorProjections(t *testing.T) {
	root := os.Getenv("QUIP_CONFORMANCE_FIXTURES")
	if root == "" {
		t.Skip("requires QUIP_CONFORMANCE_FIXTURES")
	}
	root = filepath.Join(root, "aglais-117-118")
	raw, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	require.NoError(t, err)
	require.Equal(t, "a77d59562ecab6aaf80173e70a1f6dcf210815f8af471c7ce356e7ef08bd4b89", fmt.Sprintf("%x", sha256.Sum256(raw)))
	var manifest struct {
		Blocks []struct {
			Number                   int
			MetadataFile, EventsFile string
			ExecutionSpecVersion     int
		}
		Files map[string]struct{ SHA256 string }
	}
	require.NoError(t, json.Unmarshal(raw, &manifest))
	read := func(name string) []byte {
		data, err := os.ReadFile(filepath.Join(root, name))
		require.NoError(t, err)
		require.Equal(t, manifest.Files[name].SHA256, fmt.Sprintf("%x", sha256.Sum256(data)))
		return data
	}
	count := 0
	for _, block := range manifest.Blocks {
		raw := "0x" + hex.EncodeToString(read(block.MetadataFile))
		md, err := state.RegisterCheckedMetadata(&metadata.RuntimeRaw{Spec: block.ExecutionSpecVersion, Raw: raw})
		require.NoError(t, err)
		decoded, err := substrate.DecodeEvent("0x"+hex.EncodeToString(read(block.EventsFile)), md, block.ExecutionSpecVersion)
		require.NoError(t, err)
		var events []struct {
			Module string               `json:"module_id"`
			Name   string               `json:"event_id"`
			Index  int                  `json:"extrinsic_idx"`
			Params []storage.EventParam `json:"params"`
		}
		require.NoError(t, util.UnmarshalAny(&events, decoded))
		for _, event := range events {
			if !strings.EqualFold(event.Module, "system") || event.Name != "ExtrinsicFailed" {
				continue
			}
			d := &projectionStorage{raw: raw}
			e := &storage.Event{BlockNum: block.Number, ExtrinsicIdx: event.Index}
			require.NoError(t, New(d).ExtrinsicFailed(block.ExecutionSpecVersion, e, event.Params))
			require.Equal(t, "MinerRegistry", d.row.Module)
			require.Equal(t, "DescriptorRequired", d.row.Name)
			require.Equal(t, fmt.Sprintf("%d-%d", block.Number, event.Index), d.row.ExtrinsicIndex)
			d.err = errors.New("injected persistence failure")
			require.ErrorIs(t, New(d).ExtrinsicFailed(block.ExecutionSpecVersion, e, event.Params), d.err)
			count++
		}
	}
	require.Equal(t, 11, count)
}

func TestDispatchErrorProjectionPropagatesFailure(t *testing.T) {
	d := &projectionStorage{err: errors.New("injected persistence failure")}
	s := New(d)
	e := &storage.Event{BlockNum: 42, ExtrinsicIdx: 1}
	params := []storage.EventParam{{Type: "DispatchError", Value: map[string]interface{}{"BadOrigin": nil}}}
	require.ErrorIs(t, s.ExtrinsicFailed(1, e, params), d.err)
	require.Nil(t, d.row)
	d.err = nil
	require.NoError(t, s.ExtrinsicFailed(1, e, params))
	require.Equal(t, "42-1", d.row.ExtrinsicIndex)
	require.Equal(t, "BadOrigin", d.row.Name)
	require.Error(t, s.ExtrinsicFailed(1, e, nil), "missing error details must not acknowledge a projection")
	require.Error(t, s.ExtrinsicFailed(1, e, []storage.EventParam{{Type: "DispatchError", Value: map[string]interface{}{"unrecognized": nil}}}))
}
