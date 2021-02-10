package cachex

import (
	"github.com/lixianmin/logo"
	"sync"
	"testing"
	"time"
)

/********************************************************************
created:    2021-02-10
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

func TestCache_Load(t *testing.T) {
	var cache = NewCache(1000, 10000)
	var wg sync.WaitGroup

	var count = 10
	wg.Add(count)

	for i := 0; i < count; i++ {
		go func(i int) {
			var value = cache.Load(i, func() (interface{}, time.Duration) {
				logo.JsonI("title", "loader is called", "i", i)
				return i, 5 * time.Second
			})

			logo.JsonI("title", "out", "value", value)
			wg.Done()
		}(i)
	}

	wg.Wait()
	//for i := 0; i < 10; i++ {
	//	var value = cache.Load(i, func(key interface{}) (interface{}, time.Duration) {
	//		logo.JsonI("title", "loader is called", "i", i)
	//		return i, 5 * time.Second
	//	})
	//
	//	logo.JsonI("title", "out", "value", value)
	//}
	//
	//time.Sleep(6 * time.Second)
	//
	//for i := 0; i < 10; i++ {
	//	var value = cache.Load(i, func(key interface{}) (interface{}, time.Duration) {
	//		logo.JsonI("title", "loader is called", "i", i)
	//		return i, 5 * time.Second
	//	})
	//
	//	logo.JsonI("title", "out", "value", value)
	//}
}
