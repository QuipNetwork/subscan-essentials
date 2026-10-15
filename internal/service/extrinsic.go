package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/itering/subscan/internal/dao"
	"github.com/itering/subscan/model"
	"github.com/itering/subscan/util"
	"github.com/itering/subscan/util/address"
)

func (s *Service) createExtrinsic(ctx context.Context,
	txn *dao.GormDB,
	block *model.ChainBlock,
	extrinsics []model.ChainExtrinsic,
	encodeExtrinsics []string,
	eventMap map[string][]model.ChainEvent,
) (err error) {

	var countSignedExtrinsic int

	for index, extrinsic := range extrinsics {
		extrinsics[index].BlockNum = block.BlockNum
		extrinsics[index].ExtrinsicIndex = fmt.Sprintf("%d-%d", block.BlockNum, index)
		extrinsics[index].Success = s.getExtrinsicSuccess(eventMap[extrinsics[index].ExtrinsicIndex])
		extrinsics[index].BlockTimestamp = block.BlockTimestamp
		extrinsics[index].AccountId = address.Format(extrinsic.AccountId)
		extrinsics[index].ExtrinsicHash = util.AddHex(extrinsic.ExtrinsicHash)
		extrinsics[index].ParamsRawBytes = util.HexToBytes(extrinsic.ParamsRaw)
		extrinsics[index].Params = nil
		if extrinsic.IsSigned {
			countSignedExtrinsic++
		}
		enrichExtrinsicFee(ctx, &extrinsics[index], encodeExtrinsics[index], block, eventMap[extrinsics[index].ExtrinsicIndex])

		extrinsics[index].ID = extrinsics[index].Id()
	}
	if err = s.dao.CreateExtrinsic(ctx, txn, extrinsics, countSignedExtrinsic); err != nil {
		return err
	}
	return nil
}

func (s *Service) ExtrinsicsAsJson(e *model.ChainExtrinsic) *model.ChainExtrinsicJson {
	ej := &model.ChainExtrinsicJson{
		BlockNum:           e.BlockNum,
		BlockTimestamp:     e.BlockTimestamp,
		ExtrinsicIndex:     e.ExtrinsicIndex,
		ExtrinsicHash:      e.ExtrinsicHash,
		Success:            e.Success,
		CallModule:         e.CallModule,
		CallModuleFunction: e.CallModuleFunction,
		Params:             e.Params,
		AccountId:          address.Encode(e.AccountId),
		Signature:          e.Signature,
		Nonce:              e.Nonce,
		FeeInfo:            e.FeeInfo(),
	}
	return ej
}

func FindOutBlockTime(extrinsics []model.ChainExtrinsic) int {
	for _, extrinsic := range extrinsics {
		if strings.EqualFold(extrinsic.CallModule, "timestamp") {
			params := model.ParsingExtrinsicParam(extrinsic.Params)
			for _, p := range params {
				if strings.EqualFold(p.Name, "now") {
					if strings.EqualFold(p.Type, "compact<U64>") {
						return int(util.Int64FromInterface(p.Value) / 1000)
					}
					return util.IntFromInterface(p.Value)
				}
			}
		}
	}
	return 0
}

// getExtrinsicSuccess reports confirmed outer dispatch success. Missing or
// ambiguous outcomes are not success; false alone is not proof of dispatch failure.
func (s *Service) getExtrinsicSuccess(events []model.ChainEvent) bool {
	seen, success := false, false
	for _, event := range events {
		if event.Phase != 0 || !strings.EqualFold(event.ModuleId, "system") {
			continue
		}
		isSuccess := strings.EqualFold(event.EventId, "ExtrinsicSuccess")
		isFailure := strings.EqualFold(event.EventId, "ExtrinsicFailed")
		if !isSuccess && !isFailure {
			continue
		}
		if seen {
			return false
		}
		seen, success = true, isSuccess
	}
	return seen && success
}
