package redisx

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/lixianmin/got/loom"
	"github.com/redis/go-redis/v9"
)

/********************************************************************
created:    2024-11-09
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type StreamMessage struct {
	Id     string
	Values map[string]any
	Err    error

	reader *StreamReader
}

// Ack 确认消息已被处理, 这个必须在message自己调用, 而不是直接基于返回值, 因为可能在回调方法中
func (msg *StreamMessage) Ack(ctx context.Context) error {
	if msg.reader == nil || msg.Err != nil {
		return nil // 如果是错误消息或reader为nil，直接返回
	}

	return msg.reader.ack(ctx, msg.Id)
}

type StreamReader struct {
	client    *redis.Client
	streamKey string
	wc        loom.WaitClose
	groupName string
	handler   func(context.Context, StreamMessage)
}

func NewStreamReader(client *redis.Client, streamKey string, handler func(context.Context, StreamMessage), options ...StreamReaderOption) *StreamReader {
	if handler == nil {
		panic("handler is required")
	}

	// 设置默认参数
	var serviceName = filepath.Base(os.Args[0])
	var args = streamReaderArguments{
		groupName:     serviceName,
		consumerName:  serviceName,
		blockDuration: 5 * time.Second, // 默认阻塞5秒
		startId:       ">",             // 默认只读取新消息
	}

	for _, opt := range options {
		opt(&args)
	}

	var my = &StreamReader{
		client:    client,
		streamKey: streamKey,
		groupName: args.groupName,
		handler:   handler,
	}

	// 确保消费者组存在
	ctx := context.Background()
	client.XGroupCreateMkStream(ctx, streamKey, args.groupName, "0").Err()

	loom.Go(func(later loom.Later) {
		my.goRead(later, args)
	})

	return my
}

func (my *StreamReader) goRead(later loom.Later, args streamReaderArguments) {
	defer my.Close()

	var ctx = context.Background()
	for {
		select {
		case <-my.wc.C():
			return
		default:
			// 从stream中读取消息
			streams, err := my.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    args.groupName,
				Consumer: args.consumerName,
				Streams:  []string{my.streamKey, args.startId},
				Count:    1,
				Block:    args.blockDuration,
			}).Result()

			if err != nil && err != redis.Nil {
				// 处理错误消息
				my.processMessage(StreamMessage{
					Err:    err,
					reader: my,
				})
				continue
			}

			// 处理读取到的消息
			if len(streams) > 0 && len(streams[0].Messages) > 0 {
				for _, msg := range streams[0].Messages {
					var message = StreamMessage{
						Id:     msg.ID,
						Values: msg.Values,
						reader: my,
					}

					my.processMessage(message)
				}
			}
		}
	}
}

func (my *StreamReader) processMessage(message StreamMessage) {
	var ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	my.handler(ctx, message)
}

func (my *StreamReader) Close() error {
	return my.wc.Close(nil)
}

func (my *StreamReader) ack(ctx context.Context, messageId string) error {
	return my.client.XAck(ctx, my.streamKey, my.groupName, messageId).Err()
}
