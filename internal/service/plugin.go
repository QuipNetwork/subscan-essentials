package service

import (
	"github.com/itering/subscan/internal/dao"
	"github.com/itering/subscan/plugins"
	redisDao "github.com/itering/subscan/share/redis"
)

func pluginRegister(ds *dao.DbStorage, pool *redisDao.Dao) {
	for name, plugin := range plugins.RegisteredPlugins {
		db := *ds
		db.Prefix = name
		plugin.InitDao(&db)
		plugin.SetRedisPool(pool)
	}
}

var ignoreEvent = []string{"system.ExtrinsicSuccess"}
