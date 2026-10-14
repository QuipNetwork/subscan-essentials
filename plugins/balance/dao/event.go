package dao

import (
	"context"
	"fmt"
	subscan_plugin "github.com/itering/subscan-plugin"
	"github.com/itering/subscan-plugin/storage"
	"github.com/itering/subscan/model"
	bModel "github.com/itering/subscan/plugins/balance/model"
	"github.com/itering/subscan/share/token"
	"github.com/itering/subscan/util"
	"gorm.io/gorm"
)

type Storage struct {
	Dao  storage.Dao
	Pool subscan_plugin.RedisPool
}

func EmitEvent(ctx context.Context, d *Storage, event *storage.Event, block *storage.Block) error {
	var paramEvent []storage.EventParam
	if err := util.UnmarshalAny(&paramEvent, event.Params); err != nil {
		return err
	}
	switch event.EventId {
	// [accountId, balance]
	case "Endowed", "Reserved", "Unreserved", "Deposit", "Minted", "Issued", "Locked", "Unlocked", "Withdraw":
		if len(paramEvent) < 1 {
			return fmt.Errorf("%s event missing account", event.EventId)
		}
		return RefreshAccount(ctx, d, model.CheckoutParamValueAddress(paramEvent[0].Value))
		// ["AccountId","AccountId","Balance"]
	case "Transfer":
		if len(paramEvent) < 3 {
			return fmt.Errorf("transfer event missing parameters")
		}
		from := model.CheckoutParamValueAddress(paramEvent[0].Value)
		to := model.CheckoutParamValueAddress(paramEvent[1].Value)
		balance := util.DecimalFromInterface(paramEvent[2].Value)
		t := token.GetDefaultToken()
		if t == nil {
			return fmt.Errorf("transfer token metadata unavailable")
		}
		return CreateTransfer(ctx, d, &bModel.Transfer{
			Id:             event.Id,
			Sender:         from,
			Receiver:       to,
			Amount:         balance,
			BlockNum:       uint(event.BlockNum),
			BlockTimestamp: int64(block.BlockTimestamp),
			Symbol:         t.Symbol,
			TokenId:        t.TokenId,
			ExtrinsicIndex: fmt.Sprintf("%d-%d", event.BlockNum, event.ExtrinsicIdx),
		})
	}
	return nil
}

func RefreshMetadata(ctx context.Context, d *Storage) {
	err := withCounts(ctx, d, func(tx *gorm.DB, counts *bModel.ProjectionCounts) error {
		if err := tx.Model(&bModel.Transfer{}).Count(&counts.Transfers).Error; err != nil {
			return err
		}
		if err := tx.Model(&bModel.Account{}).Count(&counts.Accounts).Error; err != nil {
			return err
		}
		return d.Pool.HmSetEx(ctx, model.MetadataCacheKey(), map[string]int64{"total_transfer": counts.Transfers, "total_account": counts.Accounts}, -1)
	})
	if err != nil {
		util.Logger().Error(fmt.Errorf("balance count publication: %w", err))
	}
}
