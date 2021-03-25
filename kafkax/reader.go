package kafkax

import (
	"context"
	"fmt"
	"github.com/lixianmin/got/loom"
	"github.com/lixianmin/logo"
	"github.com/segmentio/kafka-go"
	"os"
	"path/filepath"
	"sync/atomic"
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
	reader      atomic.Value //*kafka.Reader
	config      kafka.ReaderConfig
	messageChan chan Message
	wc          loom.WaitClose
	monitor     *readerMonitor
}

func NewReader(brokers []string, topic string, options ...ReaderOption) *Reader {
	// 我们还是很希望一眼能够看到默认值的
	var serviceName = filepath.Base(os.Args[0])
	var args = readerArguments{
		groupId:         serviceName,
		minBytes:        10e3, // 10KB
		maxBytes:        10e6, // 10MB
		startOffset:     kafka.FirstOffset,
		messageChanSize: 128,
		monitorLagLimit: time.Minute,
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
		Logger:      &logger{PrintFunc: logo.GetLogger().Info},
		ErrorLogger: &logger{PrintFunc: logo.GetLogger().Error},
	}

	// 创建对象
	var my = &Reader{
		config:      config,
		messageChan: make(chan Message, args.messageChanSize),
		monitor:     newReaderMonitory(args.monitorLagLimit),
	}

	var reader = kafka.NewReader(config)
	my.setReader(reader)
	loom.Go(my.goRead)
	return my
}

func (my *Reader) goRead(later loom.Later) {
	defer my.Close()

	var ctx = context.Background()
	for !my.wc.IsClosed() {
		var reader = my.GetReader()
		var msg, err = reader.FetchMessage(ctx)

		my.messageChan <- Message{
			Message: msg,
			Err:     err,
		}

		if err == nil {
			my.monitor.checkConsumeLag(msg)
		}
	}
}

func (my *Reader) Close() error {
	return my.wc.Close(func() error {
		var reader = my.GetReader()
		return reader.Close()
	})
}

func (my *Reader) SetOffset(offset int64) error {
	if offset < kafka.FirstOffset {
		return fmt.Errorf("invalid offset=%d", offset)
	}

	var usingGroup = my.config.GroupID != ""
	if usingGroup {
		my.config.StartOffset = offset

		var reader = my.GetReader()
		var err = reader.Close()

		var next = kafka.NewReader(my.config)
		my.setReader(next)
		return err
	} else {
		var reader = my.GetReader()
		return reader.SetOffset(offset)
	}
}

func (my *Reader) setReader(reader *kafka.Reader) {
	my.reader.Store(reader)
}

func (my *Reader) GetReader() *kafka.Reader {
	return my.reader.Load().(*kafka.Reader)
}

func (my *Reader) GetMessageChan() <-chan Message {
	return my.messageChan
}
