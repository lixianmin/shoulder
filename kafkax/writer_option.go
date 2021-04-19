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
	readTimeout  time.Duration
	writeTimeout time.Duration
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

func WithBatchTimeout(timeout time.Duration) WriterOption {
	return func(args *writerArguments) {
		args.batchTimeout = timeout
	}
}

func WithReadTimeout(timeout time.Duration) WriterOption {
	return func(args *writerArguments) {
		args.readTimeout = timeout
	}
}

func WithWriteTimeout(timeout time.Duration) WriterOption {
	return func(args *writerArguments) {
		args.writeTimeout = timeout
	}
}

func WithAsync(async bool) WriterOption {
	return func(args *writerArguments) {
		args.async = async
	}
}
