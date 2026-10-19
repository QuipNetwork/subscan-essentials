package service

import (
	"fmt"
	"github.com/itering/subscan/internal/dao"
	"github.com/itering/subscan/model"
	"github.com/itering/subscan/util"
	"strings"
)

func (s *Service) AddEvent(txn *dao.GormDB, block *model.ChainBlock, events []model.ChainEvent) (err error) {
	var inserts []model.ChainEvent
	for _, event := range events {
		e := model.ChainEvent{
			BlockNum: block.BlockNum,
			ModuleId: strings.ToLower(event.ModuleId),
			// Params:         event.Params,
			EventIdx:       event.EventIdx,
			EventId:        event.EventId,
			ExtrinsicIdx:   event.ExtrinsicIdx,
			Phase:          event.Phase,
			ParamsRawBytes: util.HexToBytes(event.ParamsRaw),
		}
		if event.Phase == 0 {
			e.ExtrinsicIndex = fmt.Sprintf("%d-%d", block.BlockNum, event.ExtrinsicIdx)
		}
		e.ID = e.Id()
		inserts = append(inserts, e)
	}
	return s.dao.CreateEvent(txn, inserts)
}
