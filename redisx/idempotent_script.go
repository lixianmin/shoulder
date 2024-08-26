package redisx

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

/********************************************************************
created:    2021-06-29
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type IdempotentScript struct {
	db            redis.Scripter
	expireSeconds string
}

func NewIdempotentScript(scripter redis.Scripter, expiration time.Duration) *IdempotentScript {
	var my = &IdempotentScript{
		db:            scripter,
		expireSeconds: strconv.FormatFloat(expiration.Seconds(), 'f', -1, 64),
	}

	return my
}

func (my *IdempotentScript) Incr(ctx context.Context, key IdempotentKey) *redis.Cmd {
	const script = `
local key, partition, offset, expire = KEYS[1], KEYS[2], tonumber(KEYS[3]), tonumber(KEYS[4]);
local idempotentKey = key..'_'..partition..'_incr';
local last = tonumber(redis.call('get', idempotentKey)) or -1;
if (last < offset) then
	redis.call('set', idempotentKey, offset);
	local result = redis.call('incr', key);
	
	if (expire > 0) then
		redis.call('expire', idempotentKey, expire);
		redis.call('expire', key, expire);
	end
	
	return result;
end

return tonumber(redis.call('get', key));
`
	var keys = key.getKeys(my.expireSeconds)
	var ret = my.db.Eval(ctx, script, keys)
	return ret
}

func (my *IdempotentScript) IncrBy(ctx context.Context, key IdempotentKey, increment int) *redis.Cmd {
	const script = `
local key, partition, offset, expire, increment = KEYS[1], KEYS[2], tonumber(KEYS[3]), tonumber(KEYS[4]), tonumber(KEYS[5]);
local idempotentKey = key..'_'..partition..'_incrby';
local last = tonumber(redis.call('get', idempotentKey)) or -1;
if (last < offset) then
	redis.call('set', idempotentKey, offset);
	local result = redis.call('incrby', key, increment);
	
	if (expire > 0) then
		redis.call('expire', idempotentKey, expire);
		redis.call('expire', key, expire);
	end
	
	return result;
end

return tonumber(redis.call('get', key));
`
	var increment1 = strconv.Itoa(increment)
	var keys = key.getKeys(my.expireSeconds, increment1)
	var ret = my.db.Eval(ctx, script, keys)
	return ret
}

func (my *IdempotentScript) LPush(ctx context.Context, key IdempotentKey, values ...interface{}) *redis.Cmd {
	const script = `
local key, partition, offset, expire = KEYS[1], KEYS[2], tonumber(KEYS[3]), tonumber(KEYS[4]);
local idempotentKey = key..'_'..partition..'_lpush';
local last = tonumber(redis.call('get', idempotentKey)) or -1;
if (last < offset) then
	redis.call('set', idempotentKey, offset);
	local result = redis.call('lpush', key, unpack(ARGV));
	
	if (expire > 0) then
		redis.call('expire', idempotentKey, expire);
		redis.call('expire', key, expire);
	end
	
	return result;
end

return redis.call('llen', key);
`
	var keys = key.getKeys(my.expireSeconds)
	var ret = my.db.Eval(ctx, script, keys, values...)
	return ret
}

func (my *IdempotentScript) ZIncrBy(ctx context.Context, key IdempotentKey, increment float64, member string) *redis.Cmd {
	const script = `
local key, partition, offset, expire, increment, member = KEYS[1], KEYS[2], tonumber(KEYS[3]), tonumber(KEYS[4]), tonumber(KEYS[5]), KEYS[6];
local idempotentKey = key..'_'..partition..'_zincrby';
local last = tonumber(redis.call('get', idempotentKey)) or -1;
if (last < offset) then
	redis.call('set', idempotentKey, offset);
	local result = redis.call('zincrby', key, increment, member);
	
	if (expire > 0) then
		redis.call('expire', idempotentKey, expire);
		redis.call('expire', key, expire);
	end
	
	return result;
end

return tonumber(redis.call('zscore', key, member));
`
	var increment1 = strconv.FormatFloat(increment, 'f', -1, 64)
	var keys = key.getKeys(my.expireSeconds, increment1, member)
	var ret = my.db.Eval(ctx, script, keys)
	return ret
}
