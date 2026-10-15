package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/itering/subscan/model"
	"github.com/itering/subscan/util"
	"github.com/itering/substrate-api-rpc"
	"github.com/itering/substrate-api-rpc/hasher"
	smodel "github.com/itering/substrate-api-rpc/model"
	"github.com/itering/substrate-api-rpc/storage"
	"strings"
)

// The final argument is retained for existing callers; an unqualified numeric
// session index cannot establish availability or the state used to obtain it.
func (s *Service) CreateChainBlock(ctx context.Context, hash string, block *smodel.Block, event string, spec int, _ uint) (err error) {
	var (
		decodeExtrinsics []map[string]interface{}
		decodeEvent      interface{}
		logs             []storage.DecoderLog
	)

	executionHash, blockNum, err := executionStateHash(hash, block)
	if err != nil {
		return err
	}
	metadataInstant, err := s.getMetadataInstant(spec, executionHash)
	if err != nil {
		return fmt.Errorf("block %d execution metadata: %w", blockNum, err)
	}

	// Extrinsic
	decodeExtrinsics, err = substrate.DecodeExtrinsic(block.Extrinsics, metadataInstant, spec)
	if err != nil {
		return fmt.Errorf("block %d decode: %w", blockNum, err)
	}
	// event
	decodeEvent, err = substrate.DecodeEvent(event, metadataInstant, spec)
	if err != nil {
		return fmt.Errorf("block %d decode: %w", blockNum, err)
	}

	// log
	logs, err = substrate.DecodeLogDigest(block.Header.Digest.Logs)
	if err != nil {
		return fmt.Errorf("block %d decode: %w", blockNum, err)
	}

	extrinsics, events, err := convertBlockData(block.Extrinsics, event, decodeExtrinsics, decodeEvent)
	if err != nil {
		return fmt.Errorf("block %d: %w", blockNum, err)
	}

	cb := model.ChainBlock{
		ID:             blockNum,
		Hash:           hash,
		BlockNum:       blockNum,
		ParentHash:     block.Header.ParentHash,
		StateRoot:      block.Header.StateRoot,
		ExtrinsicsRoot: block.Header.ExtrinsicsRoot,
		SpecVersion:    spec,
		Finalized:      true,
	}

	var preRuntime []byte
	preRuntimeCount := 0
	for _, log := range logs {
		if strings.EqualFold(log.Type, "PreRuntime") {
			preRuntimeCount++
			preRuntime, _ = json.Marshal(log.Value)
		}
	}
	if preRuntimeCount != 1 {
		preRuntime = nil
	}
	s.enrichBlockState(ctx, &cb, preRuntime)

	s.dao.SplitBlockTable(blockNum)
	txn, err := s.dao.DbBegin()
	if err != nil {
		return fmt.Errorf("block %d begin: %w", blockNum, err)
	}
	defer s.dao.DbRollback(txn)

	eventMap := s.checkoutExtrinsicEvents(events, blockNum)

	extrinsics = s.fillExtrinsicHash(blockNum, extrinsics, block.Extrinsics)
	cb.BlockTimestamp = FindOutBlockTime(extrinsics)
	err = s.createExtrinsic(ctx, txn, &cb, extrinsics, block.Extrinsics, eventMap)
	if err != nil {
		return err
	}

	if err = s.AddEvent(txn, &cb, events); err != nil {
		return err
	}

	if _, err = s.EmitLog(txn, blockNum, logs, true); err != nil {
		return err
	}

	cb.ExtrinsicsCount = len(extrinsics)
	cb.EventCount = len(events)

	if err = s.dao.CreateBlock(ctx, txn, &cb); err != nil {
		return err
	}
	if err = s.dao.CreatePluginProjections(txn, BuildPluginProjections(blockNum, events, extrinsics)); err != nil {
		return err
	}
	if err = s.dao.DbCommit(txn); err != nil {
		return fmt.Errorf("block %d commit: %w", blockNum, err)
	}
	return s.completeCoreBlock(ctx, blockNum)
}

func (s *Service) checkoutExtrinsicEvents(e []model.ChainEvent, blockNumInt uint) map[string][]model.ChainEvent {
	eventMap := make(map[string][]model.ChainEvent)
	for _, event := range e {
		if event.Phase != 0 {
			continue
		}
		extrinsicIndex := fmt.Sprintf("%d-%d", blockNumInt, event.ExtrinsicIdx)
		eventMap[extrinsicIndex] = append(eventMap[extrinsicIndex], event)
	}
	return eventMap
}

func (s *Service) GetCurrentRuntimeSpecVersion(blockNum uint) int {
	if util.CurrentRuntimeSpecVersion != 0 {
		return util.CurrentRuntimeSpecVersion
	}
	if block := s.dao.GetNearBlock(blockNum); block != nil {
		return block.SpecVersion
	}
	return -1
}

func (s *Service) GetBlockByHashJson(ctx context.Context, hash string) *model.ChainBlockJson {
	block := s.dao.GetBlockByHash(ctx, hash)
	if block == nil {
		return nil
	}
	return s.dao.BlockAsJson(ctx, block)
}

func (s *Service) fillExtrinsicHash(blockNum uint, extrinsicList []model.ChainExtrinsic, extrinsicRaws []string) []model.ChainExtrinsic {
	for i, e := range extrinsicList {
		extrinsicList[i].BlockNum = blockNum
		if util.TrimHex(e.ExtrinsicHash) == "" {
			extrinsicList[i].ExtrinsicHash = util.AddHex(util.BytesToHex(hasher.HashByCryptoName(util.HexToBytes(extrinsicRaws[i]), "Blake2_256")))
		}
		extrinsicList[i].ExtrinsicIndex = fmt.Sprintf("%d-%d", blockNum, i)
	}
	return extrinsicList
}
