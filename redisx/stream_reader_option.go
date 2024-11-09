package redisx

import "time"

type streamReaderArguments struct {
	groupName       string
	consumerName    string
	blockDuration   time.Duration
	messageChanSize int
	startId         string
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

func WithMessageChanSize(size int) StreamReaderOption {
	return func(args *streamReaderArguments) {
		args.messageChanSize = size
	}
}

func WithStartID(id string) StreamReaderOption {
	return func(args *streamReaderArguments) {
		args.startId = id
	}
}
