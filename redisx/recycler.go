package redisx

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/lixianmin/got/loom"
	"github.com/lixianmin/got/osx"
	"github.com/lixianmin/got/taskx"
	"github.com/lixianmin/got/timex"
	"github.com/lixianmin/logo"
	"github.com/spf13/cast"
	"sync"
	"time"
)

/********************************************************************
created:    2022-04-24
author:     lixianmin

v1. 如果传入的client是一个pipeline, 则很多接口无法立即拿到返回值, 不满足gc等操作的需求.
v2. 但如果不使用pipeline则有可能会超时, 改为使用异步, 但异步会受总RTT的限制, 必须改为pipeline

Copyright (C) - All Rights Reserved
*********************************************************************/

var ErrInvalidMarkTime = errors.New("invalid mark time")

const (
	KindZSet = "zset"
)

type recyclerItem struct {
	kind string
	key  string
}

type recyclerTaskArgument struct {
	pipe redis.Pipeliner
	ctx  context.Context
}

type Recycler struct {
	client     *redis.Client // 这个必须是client, 因为很多操作需要立即拿到返回值. 但是, 像ZAdd这样的操作却不能全使用client, 否则有可能卡主流程的逻辑
	expiration time.Duration // 每个key每次被访问后, 会重置过期时间
	markTime   int64         // 标记时间, 第一次运行Recycler时把当前时间写入到redis中, 作为一个兜底的refreshTime
	handler    RecyclerHandler
	seenItems  sync.Map
	tasks      *taskx.Queue
	wc         loom.WaitClose
}

func NewRecycler(client *redis.Client, options ...RecyclerOption) *Recycler {
	if client == nil {
		panic("client is nil")
	}

	var args = recyclerArguments{
		expiration:  timex.Day,
		markTimeKey: fmt.Sprintf("%s.%s.mark.time.key", osx.GetLocalIp(), osx.BaseName()),
		handler: func(kind string, key string, field string) bool {
			return false
		},
		taskQueueSize: 1024,
	}

	for _, opt := range options {
		opt(&args)
	}

	var my = &Recycler{
		client:     client,
		expiration: args.expiration,
		handler:    args.handler,
	}

	my.tasks = taskx.NewQueue(taskx.WithSize(args.taskQueueSize), taskx.WithCloseChan(my.wc.C()), taskx.WithErrorLogger(logo.GetLogger().Warn))
	my.checkSetMarkTime(args.markTimeKey)

	loom.Go(my.goLoop)
	loom.Go(func(later loom.Later) {
		my.goGarbageCollect(later, args.markTimeKey)
	})

	return my
}

func (my *Recycler) ZAdd(key string, members ...*redis.Z) {
	if len(members) == 0 {
		return
	}

	my.tasks.SendCallback(func(args interface{}) (interface{}, error) {
		var refreshTimestamp = time.Now().Unix()
		var values = make([]interface{}, 0, 2*len(members))
		for _, member := range members {
			values = append(values, member.Member, refreshTimestamp)
		}

		my.seenItems.Store(key, recyclerItem{kind: KindZSet, key: key})

		var aidKey = getRecyclerAidKey(key)
		var taskArgument = args.(recyclerTaskArgument)
		var pipe, ctx = taskArgument.pipe, taskArgument.ctx

		pipe.HSet(ctx, aidKey, values...)
		pipe.Expire(ctx, aidKey, my.expiration)
		return nil, nil
	})
}

func (my *Recycler) ZIncrBy(key string, increment float64, member string) {
	my.tasks.SendCallback(func(args interface{}) (interface{}, error) {
		var refreshTimestamp = time.Now().Unix()
		my.seenItems.Store(key, recyclerItem{kind: KindZSet, key: key})

		var aidKey = getRecyclerAidKey(key)
		var taskArgument = args.(recyclerTaskArgument)
		var pipe, ctx = taskArgument.pipe, taskArgument.ctx

		pipe.HSet(ctx, aidKey, member, refreshTimestamp)
		pipe.Expire(ctx, aidKey, my.expiration)

		return nil, nil
	})
}

func (my *Recycler) goLoop(later loom.Later) {
	var taskChan = my.tasks.C
	var closeChan = my.wc.C()

	const batchSize = 256
	var flush = NewFlushAid(batchSize, time.Second)
	var tasks = make([]taskx.Task, 0, batchSize)

	// 批量提交tasks中收集的任务
	var commitTasks = func() bool {
		if len(tasks) == 0 {
			return true
		}

		var pipe = my.client.Pipeline()
		defer pipe.Close()

		var ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		var args = recyclerTaskArgument{
			pipe: pipe,
			ctx:  ctx,
		}

		for _, task := range tasks {
			_ = task.Do(args)
		}

		if err := execPipeline(ctx, pipe); err != nil {
			logo.JsonW("err", err)
			return false
		}

		tasks = tasks[:0]
		return true
	}

	for {
		select {
		case task := <-taskChan:
			tasks = append(tasks, task)

			// 如果无法正确flush，采用back pressure策略
			for flush.CanFlush(len(tasks)) && !commitTasks() {
				time.Sleep(time.Second)
			}
		case <-closeChan:
			return
		}
	}
}

func (my *Recycler) goGarbageCollect(later loom.Later, markTimeKey string) {
	var checkInterval = minDuration(my.expiration/10, timex.Day)
	var checkTicker = later.NewTicker(checkInterval)
	var closeChan = my.wc.C()

	for {
		select {
		case <-checkTicker.C:
			my.refreshMarkTimeKey(markTimeKey)
			my.garbageCollect()
		case <-closeChan:
			return
		}
	}
}

func (my *Recycler) checkSetMarkTime(markTimeKey string) {
	var ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var client = my.client
	var ts = time.Now().Unix()
	_ = client.SetNX(ctx, markTimeKey, ts, my.expiration)
	logo.JsonI("markTimeKey", markTimeKey, "ts", ts, "expiration", my.expiration)

	if v, err := client.Get(ctx, markTimeKey).Result(); err == nil {
		my.markTime = cast.ToInt64(v)
	} else {
		logo.JsonW("markTimeKey", markTimeKey, "err", err)
	}
}

// 防止key过期
func (my *Recycler) refreshMarkTimeKey(markTimeKey string) {
	var ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	my.client.Expire(ctx, markTimeKey, my.expiration)
}

func (my *Recycler) garbageCollect() {
	my.seenItems.Range(func(key, value interface{}) bool {
		var item = value.(recyclerItem)
		switch item.kind {
		case KindZSet:
			my.recycleZSet(item)
		}
		return true
	})
}

func (my *Recycler) recycleZSet(item recyclerItem) {
	var ctx, cancel = context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var client = my.client
	var itemKey = item.key
	var members, err = my.zRangeAll(ctx, itemKey)
	if err != nil {
		if err != redis.Nil {
			logo.JsonW("itemKey", itemKey, "err", err)
		}
		return
	}

	var aidKey = getRecyclerAidKey(itemKey)
	var aidNum, err2 = client.HLen(ctx, aidKey).Result()
	if err2 != nil {
		logo.JsonW("aidKey", aidKey, "err2", err2)
		return
	}

	logo.JsonI("itemKey", itemKey, "itemNum", len(members), "aidKey", aidKey, "aidNum", aidNum)
	for _, field := range members {
		my.recycleZSetField(item, aidKey, field)
	}
}

func (my *Recycler) recycleZSetField(item recyclerItem, aidKey string, field string) {
	var ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var refreshTime, err3 = my.fetchRefreshTime(ctx, aidKey, field)
	if err3 != nil {
		logo.JsonW("aidKey", aidKey, "field", field, "err3", err3)
		return
	}

	var expireMoment = my.getExpireMoment().Unix()
	var isExpired = expireMoment > refreshTime
	if !isExpired {
		return
	}

	var needRemove = my.handler(KindZSet, item.key, field)
	if !needRemove {
		return
	}

	// 重复删除已经被删除过的field会返回0
	var _, err4 = my.client.ZRem(ctx, item.key, field).Result()
	if err4 != nil {
		logo.JsonW("key", item.key, "field", field, "err4", err4)
		return
	}

	_ = my.client.HDel(ctx, aidKey, field)
}

func (my *Recycler) zRangeAll(ctx context.Context, key string) ([]string, error) {
	var client = my.client
	var totalNum, err = client.ZCard(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	var results = make([]string, 0, totalNum)
	const step = 1000
	for i := int64(0); i < totalNum; i += step {
		var parts, err2 = client.ZRange(ctx, key, i, i+step-1).Result()
		if err2 != nil {
			return nil, err2
		}

		results = append(results, parts...)
	}

	return results, nil
}

func (my *Recycler) fetchRefreshTime(ctx context.Context, aidKey string, field string) (int64, error) {
	var v, err = my.client.HGet(ctx, aidKey, field).Result()
	// 如果入口不存在, 则使用默认的markTime
	if err == redis.Nil {
		var markTime = my.markTime
		if markTime > 0 {
			return markTime, nil
		}

		return 0, ErrInvalidMarkTime
	}

	if err != nil {
		return 0, err
	}

	var refreshTime, err2 = cast.ToInt64E(v)
	if err2 != nil {
		return 0, err2
	}

	return refreshTime, nil
}

func (my *Recycler) getExpireMoment() time.Time {
	return time.Now().Add(-my.expiration)
}

func getRecyclerAidKey(key string) string {
	return key + ".recycler.aid"
}

func minDuration(a time.Duration, b time.Duration) time.Duration {
	if a < b {
		return a
	}

	return b
}
