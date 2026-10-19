package dao

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/itering/subscan-plugin/storage"
	"github.com/itering/subscan/model"
	"github.com/itering/subscan/share/state"
	"github.com/itering/substrate-api-rpc/hasher"
	rpcmodel "github.com/itering/substrate-api-rpc/model"
	"github.com/itering/substrate-api-rpc/websocket"
	"gorm.io/gorm"
)

// InitAccount pins the finalized height reported by the storage DAO and requires
// that block to be indexed. Enumeration and reads remain at that hash.
func InitAccount(sg *Storage) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("account initialization: %v", r)
		}
	}()
	ctx := context.Background()
	height, err := sg.Dao.GetCurrentBlockNum(ctx)
	if err != nil {
		return err
	}
	blocks := sg.Dao.GetBlocksByNums(ctx, []uint{uint(height)}, "*")
	if len(blocks) != 1 || !blocks[0].Finalized || blocks[0].Hash == "" {
		return fmt.Errorf("indexed finalized block required for account initialization")
	}
	block := blocks[0]
	runtime, err := state.RuntimeVersionAt(nil, block.Hash)
	if err != nil {
		return err
	}
	m, err := state.MetadataAt(runtime.SpecVersion, block.Hash)
	if err != nil {
		return err
	}
	_, prefix, _, hashName, err := accountStorage(m, runtime.SpecVersion)
	if err != nil {
		return err
	}
	switch hashName {
	case "Blake2_128Concat", "Twox64Concat", "Identity":
	default:
		return fmt.Errorf("account enumeration requires a reversible map hasher")
	}
	prefixHex := "0x" + hex.EncodeToString(prefix)
	start := prefixHex
	for {
		request, _ := json.Marshal(rpcmodel.JsonRpcParams{Id: 19403, JsonRpc: "2.0", Method: "state_getKeysPaged", Params: []interface{}{prefixHex, 256, start, block.Hash}})
		var response struct {
			Result *[]string       `json:"result"`
			Error  *rpcmodel.Error `json:"error"`
		}
		if err := websocket.SendWsRequest(nil, &response, request); err != nil {
			return err
		}
		if response.Error != nil {
			return fmt.Errorf("account key RPC: %s", response.Error.Message)
		}
		if response.Result == nil {
			return fmt.Errorf("missing account key result")
		}
		if len(*response.Result) == 0 {
			return nil
		}
		for _, key := range *response.Result {
			if key <= start {
				return fmt.Errorf("non-increasing account key page")
			}
			raw, err := hex.DecodeString(strings.TrimPrefix(key, "0x"))
			if err != nil || len(raw) < 32 {
				return fmt.Errorf("invalid account key")
			}
			account := raw[len(raw)-32:]
			expected := append(append([]byte{}, prefix...), hasher.HashByCryptoName(account, hashName)...)
			if !bytes.Equal(raw, expected) {
				return fmt.Errorf("account key does not match metadata")
			}
			if err := RefreshAccount(ctx, sg, block, hex.EncodeToString(account)); err != nil {
				return err
			}
			start = key
		}
	}
}

func RefreshAllAccount(_ *Storage) {

}

func InitTransfer(sg *Storage) error {
	c := context.TODO()
	db := sg.Dao.GetDbInstance().(*gorm.DB)

	blockNum, err := sg.Dao.GetCurrentBlockNum(c)
	if err != nil {
		return err
	}
	for i := int(blockNum); i >= 0; i -= int(model.SplitTableBlockNum) {

		tableName := model.TableNameFromInterface(&model.ChainEvent{BlockNum: uint(i)}, db)
		var events []*model.ChainEvent

		query := db.Table(tableName).
			Where("module_id = ?", "balances").
			Where("event_id = ?", "Transfer")
		result := query.FindInBatches(&events, 50000, func(tx *gorm.DB, batch int) error {
			var blocks = make(map[int]*storage.Block)
			var blockNums []uint

			for _, e := range events {
				blockNums = append(blockNums, e.BlockNum)
			}

			for _, b := range sg.Dao.GetBlocksByNums(c, blockNums, "*") {
				blocks[b.BlockNum] = b
			}

			var extrinsicIds []string
			for _, e := range events {
				extrinsicIds = append(extrinsicIds, e.ExtrinsicIndex)
			}
			for index := range events {
				event := events[index]
				if err := EmitEvent(c, sg, event.AsPlugin(), blocks[int(event.BlockNum)]); err != nil {
					return err
				}
			}
			return nil
		})
		if result.Error != nil {
			return result.Error
		}
	}
	return nil
}
