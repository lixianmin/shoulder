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

type PushConsumer struct {
	consumer rocketmq.PushConsumer
	topic    string
	wc       loom.WaitClose
}

func NewPushConsumer(topic string, processor ConsumerHandler, opts ...consumer.Option) *PushConsumer {
	if len(topic) == 0 {
		panic("topic is empty")
	}

	if processor == nil {
		panic("process is nil")
	}

	var pushConsumer, err = newPushConsumer(topic, processor, opts...)
	if err != nil {
		panic(err)
	}

	var my = &PushConsumer{
		consumer: pushConsumer,
		topic:    topic,
	}

	return my
}

func (my *PushConsumer) Close() error {
	return my.wc.Close(func() error {
		return my.consumer.Shutdown()
	})
}

func newPushConsumer(topic string, processor ConsumerHandler, opts ...consumer.Option) (rocketmq.PushConsumer, error) {
	var group = osx.BaseName()
	var instance = osx.GetGPID(0)

	var options = append([]consumer.Option{
		consumer.WithGroupName(group),
		consumer.WithInstance(instance), // 同一个group内的多个consumer不能同名
		//consumer.WithNameServer(nameServers),
		//consumer.WithConsumerModel(consumer.Clustering), // 这个是默认值
		consumer.WithConsumeFromWhere(consumer.ConsumeFromFirstOffset),
		//consumer.WithConsumeMessageBatchMaxSize(1), // 这个是ConsumerHandler中messages这个list的最大长度
	}, opts...)

	c, _ := rocketmq.NewPushConsumer(options...)
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
