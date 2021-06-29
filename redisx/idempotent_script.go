package redisx

import (
	"context"
	"github.com/go-redis/redis/v8"
	"strconv"
	"time"
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
local last = tonumber(redis.call('get', idempotentKey)) or 0;
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

func (my *IdempotentScript) IncrBy(ctx context.Context, key IdempotentKey, delta int) *redis.Cmd {
	const script = `
local key, partition, offset, expire, delta = KEYS[1], KEYS[2], tonumber(KEYS[3]), tonumber(KEYS[4]), tonumber(KEYS[5]);
local idempotentKey = key..'_'..partition..'_incrby';
local last = tonumber(redis.call('get', idempotentKey)) or 0;
if (last < offset) then
	redis.call('set', idempotentKey, offset);
	local result = redis.call('incrby', key, delta);
	
	if (expire > 0) then
		redis.call('expire', idempotentKey, expire);
		redis.call('expire', key, expire);
	end
	
	return result;
end

return tonumber(redis.call('get', key));
`
	var delta1 = strconv.Itoa(delta)
	var keys = key.getKeys(my.expireSeconds, delta1)
	var ret = my.db.Eval(ctx, script, keys, delta1)
	return ret
}