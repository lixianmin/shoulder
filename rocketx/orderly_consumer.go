package rocketx

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/lixianmin/got/loom"
	"github.com/lixianmin/got/osx"
)

/********************************************************************
created:    2022-04-11
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type ConsumerHandler func(ctx context.Context, messages ...*primitive.MessageExt) (consumer.ConsumeResult, error)

type OrderlyConsumer struct {
	consumer rocketmq.PushConsumer
	topic    string
	wc       loom.WaitClose
}

func NewOrderlyConsumer(nameServers primitive.NamesrvAddr, topic string, processor ConsumerHandler) *OrderlyConsumer {
	var consumer, err = newOrderlyConsumer(nameServers, topic, processor)
	if err != nil {
		panic(err)
	}

	var my = &OrderlyConsumer{
		consumer: consumer,
		topic:    topic,
	}

	return my
}

func (my *OrderlyConsumer) Close() error {
	return my.wc.Close(func() error {
		return my.consumer.Shutdown()
	})
}

func newOrderlyConsumer(nameServers primitive.NamesrvAddr, topic string, processor ConsumerHandler) (rocketmq.PushConsumer, error) {
	if len(nameServers) == 0 {
		panic("nameServers is empty")
	}

	if len(topic) == 0 {
		panic("topic is empty")
	}

	if processor == nil {
		panic("process is nil")
	}

	var group = osx.BaseName()
	var instance = osx.GetGPID(0)

	c, _ := rocketmq.NewPushConsumer(
		consumer.WithGroupName(group),
		consumer.WithInstance(instance), // 同一个group内的多个consumer不能同名
		consumer.WithNameServer(nameServers),
		consumer.WithConsumerModel(consumer.Clustering), // 这个是默认值
		consumer.WithConsumeFromWhere(consumer.ConsumeFromFirstOffset),
		//consumer.WithConsumeMessageBatchMaxSize(1), // 这个是ConsumerHandler中messages这个list的最大长度
		consumer.WithConsumerOrder(true),
	)

	var err = c.Subscribe(topic, consumer.MessageSelector{}, processor)
	if err != nil {
		return nil, err
	}

	err = c.Start()
	if err != nil {
		return nil, err
	}

	return c, nil
}
