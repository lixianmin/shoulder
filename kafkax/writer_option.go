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
	async        bool
}

type WriterOption func(*writerArguments)

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

func WithAsync(async bool) WriterOption {
	return func(args *writerArguments) {
		args.async = async
	}
}
