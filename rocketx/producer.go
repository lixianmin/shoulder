package rocketx

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/lixianmin/got/convert"
	"github.com/lixianmin/got/loom"
)

/********************************************************************
created:    2022-04-08
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type Producer struct {
	producer rocketmq.Producer
	topic    string
	wc       loom.WaitClose
}

func NewProducer(nameServers primitive.NamesrvAddr, topic string) *Producer {
	var producer, err = newProducer(nameServers)
	if err != nil {
		panic(err)
	}

	var my = &Producer{
		producer: producer,
		topic:    topic,
	}

	return my
}

func (my *Producer) Send(ctx context.Context, message Message) (*primitive.SendResult, error) {
	if message == nil {
		return nil, ErrEmptyArgument
	}

	if my.wc.IsClosed() {
		return nil, ErrClosed
	}

	var msg = &primitive.Message{
		Topic: my.topic,
		Body:  convert.ToJson(message),
	}

	msg.WithShardingKey(message.GetShardingKey())
	var result, err = my.producer.SendSync(ctx, msg)
	return result, err
}

func (my *Producer) Close() error {
	return my.wc.Close(func() error {
		return my.producer.Shutdown()
	})
}

func newProducer(nameServers []string) (rocketmq.Producer, error) {
	var group = ServiceName()
	var p, err = rocketmq.NewProducer(
		producer.WithNameServer(nameServers),
		producer.WithRetry(2),
		producer.WithGroupName(group),
		producer.WithQueueSelector(producer.NewHashQueueSelector()), // 使用hash路由, 目的是为了将所有同user id的消息发送到同一个queue中.
		//producer.WithDefaultTopicQueueNums(16),                      // 线上不建议自动创建topic, 写在这只是为了覆盖默认值(4). ---- 实测无效, 自动创建的topic中queue仍然是4
	)

	if err != nil {
		return nil, err
	}

	err = p.Start()
	if err != nil {
		return nil, err
	}

	return p, nil
}
