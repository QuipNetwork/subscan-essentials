package service

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/itering/scale.go/types"
	"github.com/itering/scale.go/types/scaleBytes"
	"github.com/itering/subscan/model"
	"github.com/itering/subscan/util"
)

// convertBlockData validates the complete decoded batch before any database or
// cache writes. Raw body positions, rather than signature presentation, define
// extrinsic identity and signedness.
func convertBlockData(rawExtrinsics []string, rawEvents string, decodedExtrinsics []map[string]interface{}, decodedEvents interface{}) (extrinsics []model.ChainExtrinsic, events []model.ChainEvent, err error) {
	defer func() {
		if cause := recover(); cause != nil {
			extrinsics, events = nil, nil
			err = fmt.Errorf("invalid block encoding: %v", cause)
		}
	}()
	if len(decodedExtrinsics) != len(rawExtrinsics) {
		return nil, nil, fmt.Errorf("extrinsic count mismatch: decoded %d, expected %d", len(decodedExtrinsics), len(rawExtrinsics))
	}
	if err = util.UnmarshalAny(&extrinsics, decodedExtrinsics); err != nil {
		return nil, nil, fmt.Errorf("convert extrinsics: %w", err)
	}
	for index, decoded := range decodedExtrinsics {
		if decoded == nil || extrinsics[index].CallModule == "" || extrinsics[index].CallModuleFunction == "" {
			return nil, nil, fmt.Errorf("extrinsic index %d: missing decoded call", index)
		}
		version, versionErr := extrinsicWireVersion(rawExtrinsics[index])
		if versionErr != nil {
			return nil, nil, fmt.Errorf("extrinsic index %d: %w", index, versionErr)
		}
		// V5 General (0x45) may carry authorization, but is not a signed
		// extrinsic. Keep the explicit wire distinction even with a signature.
		extrinsics[index].IsSigned = version == 0x84 || version == 0x85
	}
	if err = util.UnmarshalAny(&events, decodedEvents); err != nil {
		return nil, nil, fmt.Errorf("convert events: %w", err)
	}
	raw, decodeErr := hex.DecodeString(strings.TrimPrefix(rawEvents, "0x"))
	if decodeErr != nil {
		return nil, nil, fmt.Errorf("invalid event hex: %w", decodeErr)
	}
	if len(raw) == 0 {
		return nil, nil, fmt.Errorf("missing event vector length")
	}
	countDecoder := types.ScaleDecoder{}
	countDecoder.Init(scaleBytes.ScaleBytes{Data: raw}, nil)
	count := countDecoder.ProcessAndUpdateData("Compact<u32>").(int)
	if len(events) != count {
		return nil, nil, fmt.Errorf("event count mismatch: decoded %d, expected %d", len(events), count)
	}
	for index, event := range events {
		if event.EventIdx != uint(index) || event.ModuleId == "" || event.EventId == "" {
			return nil, nil, fmt.Errorf("event index %d: missing or unordered decoded event", index)
		}
		if event.Phase < 0 || event.Phase > 2 || (event.Phase == 0 && (event.ExtrinsicIdx < 0 || event.ExtrinsicIdx >= len(extrinsics))) {
			return nil, nil, fmt.Errorf("event index %d: invalid phase or extrinsic index", index)
		}
	}
	return extrinsics, events, nil
}

func extrinsicWireVersion(encoded string) (version byte, err error) {
	defer func() {
		if cause := recover(); cause != nil {
			err = fmt.Errorf("invalid extrinsic header: %v", cause)
		}
	}()
	raw, err := hex.DecodeString(strings.TrimPrefix(encoded, "0x"))
	if err != nil {
		return 0, fmt.Errorf("invalid hex: %w", err)
	}
	decoder := types.ScaleDecoder{}
	decoder.Init(scaleBytes.ScaleBytes{Data: raw}, nil)
	length := decoder.ProcessAndUpdateData("Compact<u32>").(int)
	if length != decoder.Data.GetRemainingLength() || length < 1 {
		return 0, fmt.Errorf("extrinsic body length mismatch: declared %d, remaining %d", length, decoder.Data.GetRemainingLength())
	}
	version = raw[decoder.Data.Offset]
	switch version {
	case 0x04, 0x84, 0x05, 0x85, 0x45:
		return version, nil
	default:
		return 0, fmt.Errorf("unsupported extrinsic version 0x%02x", version)
	}
}
