package redisx

import (
	"context"
	"github.com/go-redis/redis/v8"
)

/********************************************************************
created:    2021-06-29
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type Cmdable interface {
	Incr(ctx context.Context, key string) *redis.IntCmd
	IncrBy(ctx context.Context, key string, value int64) *redis.IntCmd

	LPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	LTrim(ctx context.Context, key string, start, stop int64) *redis.StatusCmd

	HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd

	PFAdd(ctx context.Context, key string, els ...interface{}) *redis.IntCmd

	SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd

	ZAdd(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd
	ZIncrBy(ctx context.Context, key string, increment float64, member string) *redis.FloatCmd
}