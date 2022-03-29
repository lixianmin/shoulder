package kafkax

import (
	"github.com/lixianmin/got/loom"
	"github.com/lixianmin/logo"
	"github.com/lixianmin/shoulder/kafkax/internal"
	"github.com/segmentio/kafka-go"
	"time"
)

/********************************************************************
created:    2021-03-01
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type Writer struct {
	writer *kafka.Writer
	wc     loom.WaitClose
}

func NewWriter(brokers []string, topic string, options ...WriterOption) *Writer {
	// 我们还是很希望一眼能够看到默认值的
	// http://www.mobabel.net/%E6%80%BB%E7%BB%93kafka%E6%80%A7%E8%83%BD%E8%B0%83%E4%BC%98%E5%92%8C%E5%8F%82%E6%95%B0%E8%B0%83%E4%BC%98/
	var args = writerArguments{
		balancer:     &kafka.LeastBytes{},
		batchSize:    128,                   // 需要设置一下，其它地方要通过writer.BatchSize取这个值
		batchBytes:   1048576,               // 单批最大大小
		batchTimeout: 10 * time.Millisecond, // 默认1s：如果是同步写，则必须调小这个参数，否则每次写都要等待1s；如果是异步写，则不需要管这个参数
		readTimeout:  10 * time.Second,      //
		writeTimeout: 10 * time.Second,      //
		requiredAcks: kafka.RequireAll,      // 默认值RequireAll，等待所有ISR成员的ack之后再返回Write()方法
		async:        false,
		logger:       &internal.Logger{PrintFunc: logo.GetLogger().Info},
		errorLogger:  &internal.Logger{PrintFunc: logo.GetLogger().Error},
	}

	for _, opt := range options {
		opt(&args)
	}

	// 创建实际对象
	var writer = &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     args.balancer,
		MaxAttempts:  10,
		BatchSize:    args.batchSize,
		BatchBytes:   args.batchBytes,
		BatchTimeout: args.batchTimeout,
		ReadTimeout:  args.readTimeout,
		WriteTimeout: args.writeTimeout,
		RequiredAcks: args.requiredAcks,
		Async:        args.async,
		Completion:   nil,
		//Compression:  compress.Gzip,
		Logger:      args.logger,
		ErrorLogger: args.errorLogger,
		Transport:   nil,
	}

	var my = &Writer{}
	my.setWriter(writer)

	loom.Go(my.goLoop)
	return my
}

func (my *Writer) goLoop(later loom.Later) {
	defer my.Close()

	var writer = my.Writer()
	var metricsTicker = later.NewTicker(time.Second)

	for {
		select {
		case <-metricsTicker.C:
			internal.TakeWriterStats(writer)
		case <-my.wc.C():
			return
		}
	}
}

func (my *Writer) Close() error {
	return my.wc.Close(func() error {
		var writer = my.Writer()
		return writer.Close()
	})
}

func (my *Writer) setWriter(writer *kafka.Writer) {
	my.writer = writer
}

func (my *Writer) Writer() *kafka.Writer {
	return my.writer
}
