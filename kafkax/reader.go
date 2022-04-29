package kafkax

import (
	"context"
	"github.com/lixianmin/got/loom"
	"github.com/lixianmin/logo"
	"github.com/lixianmin/shoulder/kafkax/internal"
	"github.com/segmentio/kafka-go"
	"os"
	"path/filepath"
	"time"
)

/********************************************************************
created:    2021-03-01
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type Message struct {
	kafka.Message
	Err error
}

type Reader struct {
	reader      *kafka.Reader
	config      kafka.ReaderConfig
	messageChan chan Message
	wc          loom.WaitClose
}

func NewReader(brokers []string, topic string, options ...ReaderOption) *Reader {
	// 我们还是很希望一眼能够看到默认值的
	var serviceName = filepath.Base(os.Args[0])
	var args = readerArguments{
		groupId:         serviceName,
		minBytes:        10e3,              // 10KB
		maxBytes:        10e6,              // 10MB
		startOffset:     kafka.FirstOffset, // 设置startOffset这事，只有在第一次进程的时候起作用，后续offset值会存储到zk中，如果想重新刷一遍数据的话，需要换一个group
		messageChanSize: 128,
		monitorLagLimit: time.Minute,
		logger:          kafka.LoggerFunc(logo.GetLogger().Info),
		errorLogger:     kafka.LoggerFunc(logo.GetLogger().Error),
	}

	for _, opt := range options {
		opt(&args)
	}

	// 创建config
	var config = kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     args.groupId,
		MinBytes:    args.minBytes,
		MaxBytes:    args.maxBytes,
		StartOffset: args.startOffset,
		Logger:      args.logger,
		ErrorLogger: args.errorLogger,
	}

	// 创建对象
	var my = &Reader{
		config:      config,
		messageChan: make(chan Message, args.messageChanSize),
	}

	var reader = kafka.NewReader(config)
	my.setReader(reader)

	loom.Go(func(later loom.Later) {
		my.goRead(later, args)
	})
	return my
}

func (my *Reader) goRead(later loom.Later, args readerArguments) {
	defer my.Close()

	var ctx = context.Background()
	var lagMonitor = internal.NewReaderLagMonitor(args.monitorLagLimit)
	//var offsetMonitor = NewReaderOffsetMonitor()

	var reader = my.Reader()
	var metricsTicker = later.NewTicker(time.Second)

	for {
		select {
		case <-metricsTicker.C:
			internal.TakeReaderStats(reader, args.groupId)
			break
		case <-my.wc.C():
			return
		default:
			var msg, err = reader.FetchMessage(ctx)

			my.messageChan <- Message{
				Message: msg,
				Err:     err,
			}

			if err == nil {
				lagMonitor.CheckConsumeLag(reader, msg)
				//offsetMonitor.checkOffset(msg)
			}
		}
	}
}

func (my *Reader) Close() error {
	return my.wc.Close(func() error {
		var reader = my.Reader()
		return reader.Close()
	})
}

func (my *Reader) setReader(reader *kafka.Reader) {
	//atomic.StorePointer(&my.reader, unsafe.Pointer(reader))
	my.reader = reader
}

func (my *Reader) Reader() *kafka.Reader {
	//var p = (*kafka.Reader)(atomic.LoadPointer(&my.reader))
	//return p
	return my.reader
}

func (my *Reader) MessageChan() <-chan Message {
	return my.messageChan
}
