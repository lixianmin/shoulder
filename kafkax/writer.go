package kafkax

import (
	"github.com/segmentio/kafka-go"
)

/********************************************************************
created:    2021-03-01
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

func CreateWriter(brokers []string, topic string, options ...WriterOption) *kafka.Writer {
	var args = createWriterArguments(options)
	var writer = &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     args.balancer,
		RequiredAcks: args.requiredAcks,
		BatchSize:    args.batchSize,    // 这里也需要设置一下，其它地方要通过writer.BatchSize取这个值
		BatchTimeout: args.batchTimeout, // 默认1s，这个必须要调小，否则每次写都需要等待1s
		BatchBytes:   args.batchBytes,   // 单批最大大小
	}

	return writer
}
