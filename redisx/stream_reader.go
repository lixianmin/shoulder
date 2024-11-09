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
}

type StreamReader struct {
	client      *redis.Client
	streamKey   string
	messageChan chan StreamMessage
	wc          loom.WaitClose
	groupName   string
}

func NewStreamReader(client *redis.Client, streamKey string, options ...StreamReaderOption) *StreamReader {
	// 设置默认参数
	var serviceName = filepath.Base(os.Args[0])
	var args = streamReaderArguments{
		groupName:       serviceName,
		consumerName:    serviceName,
		blockDuration:   5 * time.Second, // 默认阻塞5秒
		messageChanSize: 128,
		startId:         ">", // 默认只读取新消息
	}

	for _, opt := range options {
		opt(&args)
	}

	var my = &StreamReader{
		client:      client,
		streamKey:   streamKey,
		messageChan: make(chan StreamMessage, args.messageChanSize),
		groupName:   args.groupName,
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
				my.messageChan <- StreamMessage{Err: err}
				continue
			}

			// 处理读取到的消息
			if len(streams) > 0 && len(streams[0].Messages) > 0 {
				for _, msg := range streams[0].Messages {
					my.messageChan <- StreamMessage{
						Id:     msg.ID,
						Values: msg.Values,
					}
				}
			}
		}
	}
}

func (my *StreamReader) Close() error {
	return my.wc.Close(nil)
}

func (my *StreamReader) MessageChan() <-chan StreamMessage {
	return my.messageChan
}

func (my *StreamReader) Ack(ctx context.Context, messageId string) error {
	return my.client.XAck(ctx, my.streamKey, my.groupName, messageId).Err()
}
