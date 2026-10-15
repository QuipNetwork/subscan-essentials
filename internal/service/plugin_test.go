package service

import (
	"github.com/itering/subscan/model"
	"github.com/itering/subscan/plugins"
	"github.com/stretchr/testify/require"
	"testing"
)

type projectionTestPlugin struct{ plugins.PluginFactory }

func (projectionTestPlugin) Enable() bool                 { return true }
func (projectionTestPlugin) SubscribeEvent() []string     { return []string{"balances", "system"} }
func (projectionTestPlugin) SubscribeExtrinsic() []string { return []string{"example"} }

func withProjectionTestPlugin(t *testing.T) {
	old := plugins.RegisteredPlugins
	plugins.RegisteredPlugins = map[string]plugins.PluginFactory{"test": projectionTestPlugin{}}
	t.Cleanup(func() { plugins.RegisteredPlugins = old })
}

func Test_emitEvent(t *testing.T) {
	withProjectionTestPlugin(t)
	events := []model.ChainEvent{{ModuleId: "Balances", EventId: "Transfer", EventIdx: 2}, {ModuleId: "System", EventId: "ExtrinsicSuccess", EventIdx: 3}}
	rows := BuildPluginProjections(42, events, nil)
	require.Len(t, rows, 2)
	require.Equal(t, "block", rows[0].Kind)
	require.Equal(t, "event", rows[1].Kind)
	require.Equal(t, "42-2", rows[1].EntityIndex)
	require.Nil(t, rows[1].QueuedAt)
	require.Nil(t, rows[1].CompletedAt)
	require.Equal(t, rows, BuildPluginProjections(42, events, nil))
	require.NotEqual(t, rows[1].ID, BuildPluginProjections(43, events, nil)[1].ID)
}

func Test_emitExtrinsic(t *testing.T) {
	withProjectionTestPlugin(t)
	rows := BuildPluginProjections(42, nil, []model.ChainExtrinsic{{CallModule: "EXAMPLE", ExtrinsicIndex: "42-1"}, {CallModule: "unsubscribed", ExtrinsicIndex: "42-2"}})
	require.Len(t, rows, 2)
	require.Equal(t, "extrinsic", rows[1].Kind)
	require.Equal(t, "42-1", rows[1].EntityIndex)
}
