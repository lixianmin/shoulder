package kafkax

import (
	"github.com/segmentio/kafka-go"
	"os"
	"path/filepath"
)

/********************************************************************
created:    2021-03-01
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

func CreateReader(brokers []string, topic string, options ...ReaderOption) *kafka.Reader {
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
	})

	return reader
}
