package redisx

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

/********************************************************************
created:    2022-03-28
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type RedisBitSet struct {
	redis *redis.Client
	key   string
}

func NewRedisBitSet(redis *redis.Client, key string) *RedisBitSet {
	if redis == nil {
		panic("redis is nil")
	}

	var my = &RedisBitSet{redis: redis, key: key}
	return my
}

func (my *RedisBitSet) Set(offsets []uint) error {
	var ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var pipe = my.redis.Pipeline()
	defer pipe.Close()

	for _, offset := range offsets {
		var cmd = pipe.SetBit(ctx, my.key, int64(offset), 1)
		var err = cmd.Err()
		if err != nil {
			return err
		}
	}

	var _, err = pipe.Exec(ctx)
	return err
}

func (my *RedisBitSet) Test(offsets []uint) (bool, error) {
	var ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var pipe = my.redis.Pipeline()
	defer pipe.Close()

	for _, offset := range offsets {
		var cmd = pipe.GetBit(ctx, my.key, int64(offset))
		var err = cmd.Err()
		if err != nil {
			return false, err
		}
	}

	var cmds, err = pipe.Exec(ctx)
	if err != nil {
		return false, err
	}

	for _, cmd := range cmds {
		var cmd1 = cmd.(*redis.IntCmd)
		if cmd1.Val() == 0 {
			return false, nil
		}
	}

	return true, nil
}

func (my *RedisBitSet) Expire(expiration time.Duration) error {
	var ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var cmd = my.redis.Expire(ctx, my.key, expiration)
	return cmd.Err()
}
