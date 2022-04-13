package rocketx

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/lixianmin/got/convert"
	"github.com/lixianmin/got/loom"
	"github.com/lixianmin/got/osx"
)

/********************************************************************
created:    2022-04-08
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type TransactionProducer struct {
	producer rocketmq.TransactionProducer
	topic    string
	wc       loom.WaitClose
}

func NewTransactionProducer(nameServers primitive.NamesrvAddr, topic string, listener primitive.TransactionListener) *TransactionProducer {
	var producer, err = newTransactionProducer(nameServers, listener)
	if err != nil {
		panic(err)
	}

	var my = &TransactionProducer{
		producer: producer,
		topic:    topic,
	}

	return my
}

func (my *TransactionProducer) Send(ctx context.Context, message Message) (*primitive.TransactionSendResult, error) {
	if message == nil {
		return nil, ErrEmptyArgument
	}

	var msg = &primitive.Message{
		Topic: my.topic,
		Body:  convert.ToJson(message),
	}

	msg.WithShardingKey(message.GetShardingKey())
	var result, err = my.producer.SendMessageInTransaction(ctx, msg)
	return result, err
}

func (my *TransactionProducer) GetProducer() rocketmq.TransactionProducer {
	return my.producer
}

func (my *TransactionProducer) Close() error {
	return my.wc.Close(func() error {
		return my.producer.Shutdown()
	})
}

func newTransactionProducer(nameServers []string, listener primitive.TransactionListener) (rocketmq.TransactionProducer, error) {
	if listener == nil {
		panic("listener is nil")
	}

	var group = osx.BaseName()
	var instance = osx.GetGPID(0)

	var p, err = rocketmq.NewTransactionProducer(listener,
		producer.WithNameServer(nameServers),
		producer.WithRetry(10),
		producer.WithQueueSelector(producer.NewHashQueueSelector()), // 使用hash路由, 目的是为了将所有同user id的消息发送到同一个queue中.
		producer.WithGroupName(group),                               // 事务消息必须加group
		producer.WithInstanceName(instance),
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
