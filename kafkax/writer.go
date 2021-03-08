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

func CreateWriter(brokers []string, topic string, options ...WriterOption) *kafka.Writer {
	// 我们还是很希望一眼能够看到默认值的
	var args = writerArguments{
		balancer:     &kafka.LeastBytes{},
		requiredAcks: kafka.RequireAll,      // 默认值RequireAll，等待所有ISR成员的ack之后再返回Write()方法
		batchSize:    128,                   // 这里也需要设置一下，其它地方要通过writer.BatchSize取这个值
		batchBytes:   1048576,               // 单批最大大小
		batchTimeout: 10 * time.Millisecond, // 默认1s，这个必须要调小，否则每次写都需要等待1s
	}

	for _, opt := range options {
		opt(&args)
	}

	// 创建实际对象
	var writer = &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     args.balancer,
		RequiredAcks: args.requiredAcks,
		BatchSize:    args.batchSize,
		BatchTimeout: args.batchTimeout,
		BatchBytes:   args.batchBytes,
	}

	return writer
}
