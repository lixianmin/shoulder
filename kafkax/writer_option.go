package kafkax

import (
	"github.com/segmentio/kafka-go"
	"time"
)

type writerArguments struct {
	balancer     kafka.Balancer
	requiredAcks kafka.RequiredAcks
	batchSize    int
	batchBytes   int64
	batchTimeout time.Duration
}

type WriterOption func(*writerArguments)

func createWriterArguments(options []WriterOption) writerArguments {
	var args = writerArguments{
		balancer:     &kafka.LeastBytes{},
		batchSize:    128,                   // 这里也需要设置一下，其它地方要通过writer.BatchSize取这个值
		batchBytes:   1048576,               // 单批最大大小
		batchTimeout: 10 * time.Millisecond, // 默认1s，这个必须要调小，否则每次写都需要等待1s
	}

	for _, opt := range options {
		opt(&args)
	}

	return args
}

func WithBalancer(balancer kafka.Balancer) WriterOption {
	return func(args *writerArguments) {
		args.balancer = balancer
	}
}

func WithRequiredAcks(requiredAcks kafka.RequiredAcks) WriterOption {
	return func(args *writerArguments) {
		args.requiredAcks = requiredAcks
	}
}

func WithBatchSize(batchSize int) WriterOption {
	return func(args *writerArguments) {
		args.batchSize = batchSize
	}
}

func WithBatchBytes(batchBytes int64) WriterOption {
	return func(args *writerArguments) {
		args.batchBytes = batchBytes
	}
}

func WithBatchTimeout(batchTimeout time.Duration) WriterOption {
	return func(args *writerArguments) {
		args.batchTimeout = batchTimeout
	}
}
