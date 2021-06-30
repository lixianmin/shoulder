package redisx

import (
	"context"
	"github.com/go-redis/redis/v8"
	"strconv"
	"testing"
	"time"
)

/********************************************************************
created:    2021-06-29
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

func createRedis() *redis.Client {
	var opts = &redis.Options{
		Addr:         "127.0.0.1:6379",
		Username:     "",
		Password:     "",
		MaxRetries:   2,
		PoolSize:     5,
		MinIdleConns: 1,
	}

	var red = redis.NewClient(opts)
	return red
}

func TestIdempotentScript_Incr(t *testing.T) {
	var red = createRedis()
	defer red.Close()

	var name = "item"

	var script = NewIdempotentScript(red, time.Hour)
	var key = IdempotentKey{
		Key:       name,
		Partition: 0,
		Offset:    1,
	}

	var ctx = context.Background()
	red.Del(ctx, key.Key)
	red.Del(ctx, key.Key+"_"+strconv.Itoa(key.Partition)+"_incr")

	script.Incr(ctx, key)
	script.Incr(ctx, key)
	var ret = script.Incr(ctx, key)

	if ret.Val().(int64) != 1 {
		t.Fail()
	}
}


func TestIdempotentScript_IncrBy(t *testing.T) {
	var red = createRedis()
	defer red.Close()

	var name = "item2"

	var script = NewIdempotentScript(red, time.Hour)
	var key = IdempotentKey{
		Key:       name,
		Partition: 0,
		Offset:    1,
	}

	var ctx = context.Background()
	red.Del(ctx, key.Key)
	red.Del(ctx, key.Key+"_"+strconv.Itoa(key.Partition)+"_incrby")

	script.IncrBy(ctx, key, 2)
	script.IncrBy(ctx, key, 3)
	var ret = script.IncrBy(ctx, key, 4)

	if ret.Val().(int64) != 2 {
		t.Fail()
	}
}

func TestIdempotentScript_ZIncrBy(t *testing.T) {
	var red = createRedis()
	defer red.Close()

	var name = "zset"

	var script = NewIdempotentScript(red, time.Hour)
	var key = IdempotentKey{
		Key:       name,
		Partition: 0,
		Offset:    1,
	}

	var ctx = context.Background()
	red.Del(ctx, key.Key)
	red.Del(ctx, key.Key+"_"+strconv.Itoa(key.Partition)+"_zincrby")

	script.ZIncrBy(ctx, key, 2.0, "hello")
	script.ZIncrBy(ctx, key, 3.0, "hello")

	var ret = red.ZCard(ctx, key.Key)
	if ret.Val() != 1 {
		t.Fail()
	}
}

func TestIdempotentScript_LPush(t *testing.T) {
	var red = createRedis()
	defer red.Close()

	var name = "list"

	var script = NewIdempotentScript(red, time.Hour)
	var key = IdempotentKey{
		Key:       name,
		Partition: 0,
		Offset:    1,
	}

	var ctx = context.Background()
	red.Del(ctx, key.Key)
	red.Del(ctx, key.Key+"_"+strconv.Itoa(key.Partition)+"_lpush")

	script.LPush(ctx, key, "hello", "world")
	script.LPush(ctx, key, "he", "tui")
	script.LPush(ctx, key, "hi", "pet")

	var world = red.LIndex(ctx, key.Key, 0).Val()
	var hello = red.LIndex(ctx, key.Key, 1).Val()

	if hello != "hello" || world != "world" {
		t.Fail()
	}
}
