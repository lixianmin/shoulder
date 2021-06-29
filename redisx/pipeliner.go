package redisx

import (
	"context"
	"github.com/go-redis/redis/v8"
	"time"
)

/********************************************************************
created:    2021-06-29
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type Pipeliner struct {
	db         redis.Pipeliner
	expiration time.Duration
}

func NewPipeliner(pipeliner redis.Pipeliner, expiration time.Duration) *Pipeliner {
	var my = &Pipeliner{
		db:         pipeliner,
		expiration: expiration,
	}

	return my
}

func (my *Pipeliner) Close() error {
	return my.db.Close()
}

func (my *Pipeliner) Exec(ctx context.Context) ([]redis.Cmder, error) {
	return my.db.Exec(ctx)
}

func (my *Pipeliner) HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	var ret = my.db.HSet(ctx, key, values...)
	my.checkExpire(ctx, key)
	return ret
}

func (my *Pipeliner) Incr(ctx context.Context, key string) *redis.IntCmd {
	var ret = my.db.Incr(ctx, key)
	my.checkExpire(ctx, key)
	return ret
}

func (my *Pipeliner) IncrBy(ctx context.Context, key string, value int64) *redis.IntCmd {
	var ret = my.db.IncrBy(ctx, key, value)
	my.checkExpire(ctx, key)
	return ret
}

func (my *Pipeliner) LPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	var ret = my.db.LPush(ctx, key, values...)
	my.checkExpire(ctx, key)
	return ret
}

func (my *Pipeliner) LTrim(ctx context.Context, key string, start, stop int64) *redis.StatusCmd {
	var ret = my.db.LTrim(ctx, key, start, stop)
	my.checkExpire(ctx, key)
	return ret
}

func (my *Pipeliner) PFAdd(ctx context.Context, key string, els ...interface{}) *redis.IntCmd {
	var ret = my.db.PFAdd(ctx, key, els...)
	my.checkExpire(ctx, key)
	return ret
}

func (my *Pipeliner) PFCount(ctx context.Context, keys ...string) *redis.IntCmd {
	var ret = my.db.PFCount(ctx, keys...)
	return ret
}

func (my *Pipeliner) SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	var ret = my.db.SAdd(ctx, key, members...)
	my.checkExpire(ctx, key)
	return ret
}

func (my *Pipeliner) ZAdd(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd {
	var ret = my.db.ZAdd(ctx, key, members...)
	my.checkExpire(ctx, key)
	return ret
}

func (my *Pipeliner) ZIncrBy(ctx context.Context, key string, increment float64, member string) *redis.FloatCmd {
	var ret = my.db.ZIncrBy(ctx, key, increment, member)
	my.checkExpire(ctx, key)
	return ret
}

func (my *Pipeliner) checkExpire(ctx context.Context, key string) {
	if my.expiration > 0 {
		my.db.Expire(ctx, key, my.expiration)
	}
}
