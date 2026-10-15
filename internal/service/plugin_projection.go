package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/itering/subscan-plugin/storage"
	"github.com/itering/subscan/internal/dao"
	"github.com/itering/subscan/model"
	"github.com/itering/subscan/plugins"
	"github.com/itering/subscan/util"
	"github.com/itering/subscan/util/mq"
	"github.com/shopspring/decimal"
)

// BuildPluginProjections freezes enabled subscriptions at ingestion time. Stable
// IDs make a repeated block insert preserve both pending and completed work.
func BuildPluginProjections(blockNum uint, events []model.ChainEvent, extrinsics []model.ChainExtrinsic) []model.PluginProjection {
	var tasks []model.PluginProjection
	add := func(plugin, kind, index string) {
		id := fmt.Sprintf("%x", sha256.Sum256([]byte(plugin+"\x00"+kind+"\x00"+index)))
		tasks = append(tasks, model.PluginProjection{ID: id, BlockNum: blockNum, PluginName: plugin, Kind: kind, EntityIndex: index})
	}
	for name, p := range plugins.RegisteredPlugins {
		if !p.Enable() {
			continue
		}
		add(name, "block", fmt.Sprint(blockNum))
		for _, e := range events {
			if util.StringInSliceFold(e.ModuleId, p.SubscribeEvent()) && !util.StringInSliceFold(e.ModuleId+"."+e.EventId, ignoreEvent) {
				add(name, "event", fmt.Sprintf("%d-%d", blockNum, e.EventIdx))
			}
		}
		for _, e := range extrinsics {
			if util.StringInSliceFold(e.CallModule, p.SubscribeExtrinsic()) {
				add(name, "extrinsic", e.ExtrinsicIndex)
			}
		}
	}
	return tasks
}

// DispatchPluginProjections can be called after commit, on retry, or by the
// startup/periodic sweep. Queue availability never erases durable pending work.
func DispatchPluginProjections(ctx context.Context, d dao.IDao, queue mq.IJob, blockNum *uint) error {
	rows, err := d.PendingPluginProjections(ctx, blockNum, 256)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	if queue == nil {
		return errors.New("projection queue unavailable")
	}
	var failures []error
	for _, row := range rows {
		reserved, err := d.ReservePluginProjection(ctx, row.ID)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if !reserved {
			continue
		}
		// Bypass best-effort rate limiting: the SQL reservation handles delivery
		// throttling and remains pending until the worker acknowledges processing.
		err = queue.ForcePublish("plugin-projection", "process", map[string]interface{}{"projection_id": row.ID})
		failure := ""
		if err != nil {
			failure = err.Error()
			failures = append(failures, err)
		}
		if err := d.RecordProjectionDelivery(ctx, row.ID, failure); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func ProcessPluginProjection(ctx context.Context, d dao.IDao, id string) error {
	return d.ProcessPluginProjection(ctx, id, func(row model.PluginProjection) error {
		p, ok := plugins.RegisteredPlugins[row.PluginName]
		if !ok || !p.Enable() {
			return fmt.Errorf("projection plugin %q unavailable", row.PluginName)
		}
		block := d.GetBlockByNum(ctx, row.BlockNum)
		if block == nil || !block.Finalized || block.CodecError {
			return fmt.Errorf("projection block %d unavailable", row.BlockNum)
		}
		switch row.Kind {
		case "block":
			return p.ProcessBlock(ctx, block.AsPlugin())
		case "event":
			e := d.GetEventByIdx(ctx, row.EntityIndex)
			if e == nil {
				return fmt.Errorf("projection event %s unavailable", row.EntityIndex)
			}
			return p.ProcessEvent(block.AsPlugin(), e.AsPlugin(), decimal.Zero)
		case "extrinsic":
			e := d.GetExtrinsicsByIndex(ctx, row.EntityIndex)
			if e == nil {
				return fmt.Errorf("projection extrinsic %s unavailable", row.EntityIndex)
			}
			events := d.GetEventsByIndex(row.EntityIndex)
			var converted []storage.Event
			for _, event := range events {
				converted = append(converted, *event.AsPlugin())
			}
			return p.ProcessExtrinsic(block.AsPlugin(), e.AsPlugin(), converted)
		default:
			return fmt.Errorf("unknown projection kind %q", row.Kind)
		}
	})
}

func (s *Service) RunProjectionRecovery(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		if err := DispatchPluginProjections(ctx, s.dao, mq.Instant, nil); err != nil && ctx.Err() == nil {
			util.Logger().Error(fmt.Errorf("projection recovery: %w", err))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) completeCoreBlock(ctx context.Context, blockNum uint) error {
	publicationErr := s.dao.PublishBlockCompletion(ctx, blockNum)
	if err := DispatchPluginProjections(ctx, s.dao, mq.Instant, &blockNum); err != nil {
		util.Logger().Error(fmt.Errorf("committed block %d projections pending: %w", blockNum, err))
	}
	if publicationErr != nil {
		return fmt.Errorf("committed block %d publication: %w", blockNum, publicationErr)
	}
	return nil
}
