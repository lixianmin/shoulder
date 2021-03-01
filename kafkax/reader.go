package kafkax

import (
	"github.com/segmentio/kafka-go"
)

/********************************************************************
created:    2021-03-01
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

func CreateReader(brokers []string, topic string, options ...ReaderOption) *kafka.Reader {
	var args = createReaderArguments(options)
	var reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  args.groupId,
		MinBytes: args.minBytes,
		MaxBytes: args.maxBytes,
	})

	return reader
}
