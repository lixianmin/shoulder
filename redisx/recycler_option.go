package redisx

import "time"

// RecyclerHandler 返回true时执行正在的删除动作, 返回false时则不删除
type RecyclerHandler func(kind string, key string, field string) bool

type recyclerArguments struct {
	expiration    time.Duration
	markTimeKey   string
	handler       RecyclerHandler
	taskQueueSize int
}

type RecyclerOption func(*recyclerArguments)

func WithRecyclerExpiration(expiration time.Duration) RecyclerOption {
	return func(args *recyclerArguments) {
		args.expiration = expiration
	}
}

func WithRecyclerMarkTimeKey(key string) RecyclerOption {
	return func(args *recyclerArguments) {
		if key != "" {
			args.markTimeKey = key
		}
	}
}

func WithRecyclerHandler(handler RecyclerHandler) RecyclerOption {
	return func(args *recyclerArguments) {
		if handler != nil {
			args.handler = handler
		}
	}
}

func WithRecyclerTaskQueueSize(size int) RecyclerOption {
	return func(args *recyclerArguments) {
		if size > 0 {
			args.taskQueueSize = size
		}
	}
}
