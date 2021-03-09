package kafkax

import (
	"context"
	"github.com/segmentio/kafka-go"
	"os"
	"path/filepath"
)

/********************************************************************
created:    2021-03-01
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type Reader struct {
	reader      *kafka.Reader
	uncommitted []kafka.Message
}

func NewReader(brokers []string, topic string, options ...ReaderOption) *Reader {
	// 我们还是很希望一眼能够看到默认值的
	var serviceName = filepath.Base(os.Args[0])
	var args = readerArguments{
		groupId:  serviceName,
		minBytes: 10e3, // 10KB
		maxBytes: 10e6, // 10MB
	}

	for _, opt := range options {
		opt(&args)
	}

	// 创建对象
	var reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  args.groupId,
		MinBytes: args.minBytes,
		MaxBytes: args.maxBytes,
		Logger:   &Logger{},
	})

	var my = &Reader{
		reader:      reader,
		uncommitted: make([]kafka.Message, 0, 16),
	}

	return my
}

func (my *Reader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	var msg, err = my.reader.FetchMessage(ctx)
	if err != nil {
		return msg, err
	}

	my.uncommitted = append(my.uncommitted, msg)
	return msg, nil
}

func (my *Reader) CommitMessages(ctx context.Context) error {
	var count = len(my.uncommitted)
	if count == 0 {
		return nil
	}

	if err := my.reader.CommitMessages(ctx, my.uncommitted...); err != nil {
		return err
	}

	my.uncommitted = my.uncommitted[:0]
	return nil
}

func (my *Reader) GetUncommittedCount() int {
	return len(my.uncommitted)
}

func (my *Reader) GetReader() *kafka.Reader {
	return my.reader
}
