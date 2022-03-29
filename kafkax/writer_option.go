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
	logger       kafka.Logger
	errorLogger  kafka.Logger
}

type WriterOption func(*writerArguments)

func WithWriterBalancer(balancer kafka.Balancer) WriterOption {
	return func(args *writerArguments) {
		args.balancer = balancer
	}
}

func WithWriterRequiredAcks(requiredAcks kafka.RequiredAcks) WriterOption {
	return func(args *writerArguments) {
		args.requiredAcks = requiredAcks
	}
}

func WithWriterBatchSize(batchSize int) WriterOption {
	return func(args *writerArguments) {
		args.batchSize = batchSize
	}
}

func WithWriterBatchBytes(batchBytes int64) WriterOption {
	return func(args *writerArguments) {
		args.batchBytes = batchBytes
	}
}

func WithWriterBatchTimeout(timeout time.Duration) WriterOption {
	return func(args *writerArguments) {
		args.batchTimeout = timeout
	}
}

func WithWriterReadTimeout(timeout time.Duration) WriterOption {
	return func(args *writerArguments) {
		args.readTimeout = timeout
	}
}

func WithWriterWriteTimeout(timeout time.Duration) WriterOption {
	return func(args *writerArguments) {
		args.writeTimeout = timeout
	}
}

func WithWriterAsync(async bool) WriterOption {
	return func(args *writerArguments) {
		args.async = async
	}
}

func WithWriterLogger(logger kafka.Logger) WriterOption {
	return func(args *writerArguments) {
		if logger != nil {
			args.logger = logger
		}
	}
}

func WithWriterErrorLogger(logger kafka.Logger) WriterOption {
	return func(args *writerArguments) {
		if logger != nil {
			args.errorLogger = logger
		}
	}
}
