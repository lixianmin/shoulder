package redisx

import (
	"time"
)

/********************************************************************
created:    2024-11-09
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type streamReaderArguments struct {
	groupName     string
	consumerName  string
	blockDuration time.Duration
	startId       string
}

type StreamReaderOption func(*streamReaderArguments)

func WithGroupName(groupName string) StreamReaderOption {
	return func(args *streamReaderArguments) {
		args.groupName = groupName
	}
}

func WithConsumerName(consumerName string) StreamReaderOption {
	return func(args *streamReaderArguments) {
		args.consumerName = consumerName
	}
}

func WithBlockDuration(duration time.Duration) StreamReaderOption {
	return func(args *streamReaderArguments) {
		args.blockDuration = duration
	}
}

func WithStartId(id string) StreamReaderOption {
	return func(args *streamReaderArguments) {
		args.startId = id
	}
}
