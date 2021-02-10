package cachex

import (
	"github.com/dgraph-io/ristretto"
	"github.com/lixianmin/got/loom"
	"sync"
	"time"
)

/********************************************************************
created:    2021-02-10
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

var cacheSharding = loom.NewSharding()

type Cache struct {
	cache *ristretto.Cache
	locks []sync.Mutex
}

// numCounters：用于统计访问频率信息的计数器，通常比缓存max capacity要大，比如10倍
// maxCost：缓存占用的内存大小，单位: 字节
func NewCache(numCounters int64, maxCost int64, opts ...Option) *Cache {
	var config = createConfig(opts)
	config.NumCounters = numCounters
	config.MaxCost = maxCost
	var cache, err = ristretto.NewCache(config)

	if err != nil {
		panic(err)
	}

	var my = &Cache{
		cache: cache,
		locks: make([]sync.Mutex, cacheSharding.GetShardingCount()),
	}

	return my
}

func (my *Cache) Load(key interface{}, loader func() (interface{}, time.Duration)) interface{} {
	var cache = my.cache
	var value, ok = cache.Get(key)
	if !ok {
		var index, _ = cacheSharding.GetShardingIndex(key)
		my.locks[index].Lock()
		defer my.locks[index].Unlock()

		value, ok = cache.Get(key)
		if !ok {
			var ttl time.Duration
			value, ttl = loader()
			cache.SetWithTTL(key, value, 0, ttl)
		}
	}

	return value
}

func (my *Cache) GetCache() *ristretto.Cache {
	return my.cache
}
