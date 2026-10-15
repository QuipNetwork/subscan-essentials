package service

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/itering/subscan-plugin/storage"
	"github.com/itering/subscan/plugins/system/dao"
	"github.com/itering/subscan/plugins/system/model"
	"github.com/itering/subscan/util"
)

type Service struct {
	dao storage.Dao
}

func New(d storage.Dao) *Service {
	return &Service{
		dao: d,
	}
}

func (s *Service) GetExtrinsicError(hash string) *model.ExtrinsicError {
	return dao.ExtrinsicError(s.dao, hash)
}

func (s *Service) ExtrinsicFailed(spec int, event *storage.Event, paramEvent []storage.EventParam) error {
	for _, param := range paramEvent {
		// Portable metadata retains the Rust namespace in the decoded type.
		if param.Type != "DispatchError" && param.Type != "sp_runtime:DispatchError" {
			continue
		}
		moduleIndex, errorIndex, name, err := dispatchError(param.Value)
		if err != nil {
			return fmt.Errorf("dispatch error at %s: %w", genExtrinsicIndex(event), err)
		}
		detail := &model.MetadataModuleError{Name: name}
		if name == "" {
			detail = dao.CheckExtrinsicError(spec, s.dao.SpecialMetadata(spec), moduleIndex, errorIndex)
		}
		return dao.CreateExtrinsicError(s.dao, genExtrinsicIndex(event), detail)
	}
	return fmt.Errorf("unsupported dispatch error at %s", genExtrinsicIndex(event))
}

// Fieldless DispatchError and nested error enums in SDK 845eb998 (runtime 119).
// Keep the category in the stored name: these are not pallet/module errors.
var dispatchErrorNames = map[string][]string{
	"":              {"Other", "CannotLookup", "BadOrigin", "ConsumerRemaining", "NoProviders", "TooManyConsumers", "Exhausted", "Corruption", "Unavailable", "RootNotAllowed"},
	"Arithmetic":    {"Underflow", "Overflow", "DivisionByZero"},
	"Token":         {"FundsUnavailable", "OnlyProvider", "BelowMinimum", "CannotCreate", "UnknownAsset", "Frozen", "Unsupported", "CannotCreateHold", "NotExpendable", "Blocked"},
	"Transactional": {"LimitReached", "NoLayer"},
	"Trie":          {"InvalidStateRoot", "IncompleteDatabase", "ValueAtIncompleteKey", "DecoderError", "InvalidHash", "DuplicateKey", "ExtraneousNode", "ExtraneousValue", "ExtraneousHashReference", "InvalidChildReference", "ValueMismatch", "IncompleteProof", "RootMismatch", "DecodeError"},
}

// dispatchError validates named variants and accepts legacy numeric module
// errors and portable ModuleError's four SCALE bytes. The first byte is the pallet error variant; the remaining
// bytes may contain variant fields and must not be interpreted as an integer.
func dispatchError(value interface{}) (moduleIndex, errorIndex int, name string, err error) {
	bad := func() (int, int, string, error) {
		return 0, 0, "", fmt.Errorf("unsupported or malformed dispatch error")
	}
	if text, ok := value.(string); ok {
		if slices.Contains(dispatchErrorNames[""], text) {
			return 0, 0, text, nil
		}
		return bad()
	}
	var fields map[string]json.RawMessage
	if err := util.UnmarshalAny(&fields, value); err != nil {
		return bad()
	}
	byteIndex := func(raw json.RawMessage) (int, bool) {
		var n *uint8
		if json.Unmarshal(raw, &n) != nil || n == nil {
			return 0, false
		}
		return int(*n), true
	}
	if raw, ok := fields["Error"]; ok && len(fields) == 2 {
		m, mok := byteIndex(fields["Module"])
		e, eok := byteIndex(raw)
		if mok && eok {
			return m, e, "", nil
		}
		return bad()
	}
	if len(fields) != 1 {
		return bad()
	}
	if raw, ok := fields["Module"]; ok {
		var module map[string]json.RawMessage
		if json.Unmarshal(raw, &module) != nil || len(module) != 2 {
			return bad()
		}
		m, ok := byteIndex(module["index"])
		if !ok {
			return bad()
		}
		if e, ok := byteIndex(module["error"]); ok {
			return m, e, "", nil
		}
		var encoded string
		if json.Unmarshal(module["error"], &encoded) != nil || !strings.HasPrefix(encoded, "0x") {
			return bad()
		}
		data, err := hex.DecodeString(encoded[2:])
		if err != nil || len(data) != 4 {
			return bad()
		}
		return m, int(data[0]), "", nil
	}
	for _, name := range dispatchErrorNames[""] {
		// The native portable unit codec emits the literal string "NULL";
		// older decoders used JSON null or a bare variant name.
		if raw, ok := fields[name]; ok && (string(raw) == "null" || string(raw) == `"NULL"`) {
			return 0, 0, name, nil
		}
	}
	for kind, names := range dispatchErrorNames {
		if kind == "" {
			continue
		}
		if raw, ok := fields[kind]; ok {
			var name string
			if json.Unmarshal(raw, &name) == nil && slices.Contains(names, name) {
				return 0, 0, kind + "." + name, nil
			}
		}
	}
	return bad()
}

func genExtrinsicIndex(event *storage.Event) string {
	return fmt.Sprint(event.BlockNum, "-", event.ExtrinsicIdx)
}
