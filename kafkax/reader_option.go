package kafkax

import (
	"github.com/segmentio/kafka-go"
	"time"
)

/********************************************************************
created:    2021-03-01
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type readerArguments struct {
	groupId         string
	minBytes        int
	maxBytes        int
	startOffset     int64
	messageChanSize int
	monitorLagLimit time.Duration
	logger          kafka.Logger
	errorLogger     kafka.Logger
}

type ReaderOption func(*readerArguments)

func WithReaderGroupId(groupId string) ReaderOption {
	return func(args *readerArguments) {
		args.groupId = groupId
	}
}

func WithReaderMinBytes(minBytes int) ReaderOption {
	return func(args *readerArguments) {
		args.minBytes = minBytes
	}
}

func WithReaderMaxBytes(maxBytes int) ReaderOption {
	return func(args *readerArguments) {
		args.maxBytes = maxBytes
	}
}

// WithReaderStartOffset 设置startOffset这事，只有在第一次进程的时候起作用，后续offset值会存储到zk中，如果想重新刷一遍数据的话，需要换一个group
func WithReaderStartOffset(startOffset int64) ReaderOption {
	return func(args *readerArguments) {
		args.startOffset = startOffset
	}
}

func WithReaderMessageChanSize(size int) ReaderOption {
	return func(args *readerArguments) {
		if size > 0 {
			args.messageChanSize = size
		}
	}
}

func WithReaderMonitorLagLimit(lag time.Duration) ReaderOption {
	return func(args *readerArguments) {
		if lag > 0 {
			args.monitorLagLimit = lag
		}
	}
}

func WithReaderLogger(logger kafka.Logger) ReaderOption {
	return func(args *readerArguments) {
		if logger != nil {
			args.logger = logger
		}
	}
}

func WithReaderErrorLogger(logger kafka.Logger) ReaderOption {
	return func(args *readerArguments) {
		if logger != nil {
			args.errorLogger = logger
		}
	}
}
