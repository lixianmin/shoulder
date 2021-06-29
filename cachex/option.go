package cachex

import "github.com/dgraph-io/ristretto"

/********************************************************************
created:    2021-02-10
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type Option func(config *ristretto.Config)

func createConfig(options []Option) *ristretto.Config {
	var config = &ristretto.Config{
		BufferItems: 64,
	}

	for _, opt := range options {
		opt(config)
	}

	return config
}

func WithBufferItems(bufferItems int64) Option {
	return func(config *ristretto.Config) {
		if bufferItems > 0 {
			config.BufferItems = bufferItems
		}
	}
}

func WithMetrics(metrics bool) Option {
	return func(config *ristretto.Config) {
		config.Metrics = metrics
	}
}

func WithOnEvict(onEvict func(item *ristretto.Item)) Option {
	return func(config *ristretto.Config) {
		config.OnEvict = onEvict
	}
}

func WithKeyToHash(keyToHash func(key interface{}) (uint64, uint64)) Option {
	return func(config *ristretto.Config) {
		config.KeyToHash = keyToHash
	}
}

func WithCost(cost func(value interface{}) int64) Option {
	return func(config *ristretto.Config) {
		config.Cost = cost
	}
}
