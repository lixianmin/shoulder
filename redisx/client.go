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

type HookFunc func(ctx context.Context, key string)

type Client struct {
	db       redis.Cmdable
	postHook HookFunc
}

func NewClient(db redis.Cmdable, postHook HookFunc) *Client {
	var my = &Client{
		db:       db,
		postHook: postHook,
	}

	return my
}

func (my *Client) DB() redis.Cmdable {
	return my.db
}

func (my *Client) Incr(ctx context.Context, key string) *redis.IntCmd {
	var ret = my.db.Incr(ctx, key)
	my.checkPostHook(ctx, key)
	return ret
}

func (my *Client) IncrBy(ctx context.Context, key string, value int64) *redis.IntCmd {
	var ret = my.db.IncrBy(ctx, key, value)
	my.checkPostHook(ctx, key)
	return ret
}

func (my *Client) HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	var ret = my.db.HSet(ctx, key, values...)
	my.checkPostHook(ctx, key)
	return ret
}

func (my *Client) LPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	var ret = my.db.LPush(ctx, key, values...)
	my.checkPostHook(ctx, key)
	return ret
}

func (my *Client) LTrim(ctx context.Context, key string, start, stop int64) *redis.StatusCmd {
	var ret = my.db.LTrim(ctx, key, start, stop)
	my.checkPostHook(ctx, key)
	return ret
}

func (my *Client) PFAdd(ctx context.Context, key string, els ...interface{}) *redis.IntCmd {
	var ret = my.db.PFAdd(ctx, key, els...)
	my.checkPostHook(ctx, key)
	return ret
}

func (my *Client) SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	var ret = my.db.SAdd(ctx, key, members...)
	my.checkPostHook(ctx, key)
	return ret
}

func (my *Client) Set(ctx context.Context, key string, value int, expiration time.Duration) *redis.StatusCmd {
	var ret = my.db.Set(ctx, key, value, expiration)
	my.checkPostHook(ctx, key)
	return ret
}

func (my *Client) ZAdd(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd {
	var ret = my.db.ZAdd(ctx, key, members...)
	my.checkPostHook(ctx, key)
	return ret
}

func (my *Client) ZIncrBy(ctx context.Context, key string, increment float64, member string) *redis.FloatCmd {
	var ret = my.db.ZIncrBy(ctx, key, increment, member)
	my.checkPostHook(ctx, key)
	return ret
}

func (my *Client) checkPostHook(ctx context.Context, key string) {
	if my.postHook != nil {
		my.postHook(ctx, key)
	}
}
