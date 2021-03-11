package kafkax

import (
	"context"
	"github.com/lixianmin/got/loom"
	"github.com/lixianmin/logo"
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
	messageChan chan Message
	wc          loom.WaitClose

	monitorState   int
	monitorLag     time.Duration
	monitorCounter int
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
		monitorLag:      time.Minute,
	}

	for _, opt := range options {
		opt(&args)
	}

	// 创建对象
	var reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     args.groupId,
		MinBytes:    args.minBytes,
		MaxBytes:    args.maxBytes,
		StartOffset: args.startOffset,
		Logger:      &logger{PrintFunc: logo.GetLogger().Info},
		ErrorLogger: &logger{PrintFunc: logo.GetLogger().Error},
	})

	var my = &Reader{
		reader:       reader,
		messageChan:  make(chan Message, args.messageChanSize),
		monitorState: MonitorStateNormal,
		monitorLag:   args.monitorLag,
	}

	loom.Go(my.goRead)
	return my
}

func (my *Reader) goRead(later loom.Later) {
	defer my.Close()

	var ctx = context.Background()
	for !my.wc.IsClosed() {
		var msg, err = my.reader.FetchMessage(ctx)

		my.messageChan <- Message{
			Message: msg,
			Err:     err,
		}

		if err == nil {
			my.monitorConsumeLag(msg)
		}
	}
}

// 监控消费延迟
func (my *Reader) monitorConsumeLag(msg kafka.Message) {
	const changeStateCount = 5
	var delta = time.Now().Sub(msg.Time)
	var monitorLag = my.monitorLag

	if my.monitorState == MonitorStateNormal {
		if delta > monitorLag {
			my.monitorCounter++
			if my.monitorCounter > changeStateCount {
				logo.JsonW("lastMonitorState", MonitorStateNormal, "nextMonitorState", MonitorStateLagging, "monitorLag", monitorLag)
				my.monitorState = MonitorStateLagging
				my.monitorCounter = 0
			}
		}
	} else if my.monitorState == MonitorStateLagging {
		if delta < monitorLag {
			my.monitorCounter++
			if my.monitorCounter > changeStateCount {
				logo.JsonW("lastMonitorState", MonitorStateLagging, "nextMonitorState", MonitorStateNormal, "monitorLag", monitorLag)
				my.monitorState = MonitorStateNormal
				my.monitorCounter = 0
			}
		}
	}
}

func (my *Reader) Close() error {
	return my.wc.Close(func() error {
		return my.reader.Close()
	})
}

func (my *Reader) GetReader() *kafka.Reader {
	return my.reader
}

func (my *Reader) GetMessageChan() <-chan Message {
	return my.messageChan
}
