package redisx

import (
	"context"
	"fmt"
	"time"

	"github.com/lixianmin/got/loom"
	"github.com/lixianmin/logo"
	"github.com/redis/go-redis/v9"
)

/********************************************************************
created:    2024-11-09
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type StreamWriter struct {
	client    *redis.Client
	streamKey string
	args      streamWriterArguments
	wc        loom.WaitClose
}

func NewStreamWriter(client *redis.Client, streamKey string, options ...StreamWriterOption) *StreamWriter {
	var args = streamWriterArguments{
		maxLen:      1000000,            // 默认100万条消息
		maxAge:      7 * 24 * time.Hour, // 默认保存7天
		logger:      logo.GetLogger().Info,
		errorLogger: logo.GetLogger().Error,
	}

	for _, opt := range options {
		opt(&args)
	}

	var my = &StreamWriter{
		client:    client,
		streamKey: streamKey,
		args:      args,
	}

	loom.Go(my.goLoop)
	return my
}

func (my *StreamWriter) goLoop(later loom.Later) {
	defer my.Close()

	var cleanTicker = later.NewTicker(time.Hour) // 每小时清理一次过期消息
	var ctx = context.Background()

	for {
		select {
		case <-cleanTicker.C:
			my.cleanOldMessages(ctx)
		case <-my.wc.C():
			return
		}
	}
}

// Write 写入单条消息
func (my *StreamWriter) Write(ctx context.Context, values map[string]any) (string, error) {
	// 使用 XADD 命令写入消息
	id, err := my.client.XAdd(ctx, &redis.XAddArgs{
		Stream: my.streamKey,
		MaxLen: my.args.maxLen,
		Approx: true, // 使用 ~ 表示近似值
		Values: values,
	}).Result()

	if err != nil {
		if my.args.errorLogger != nil {
			my.args.errorLogger("Failed to write message:", err)
		}

		return "", err
	}

	return id, nil
}

// WriteBatch 批量写入消息
func (my *StreamWriter) WriteBatch(ctx context.Context, messages []map[string]any) error {
	pipe := my.client.Pipeline()

	for _, msg := range messages {
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: my.streamKey,
			MaxLen: my.args.maxLen,
			Approx: true,
			Values: msg,
		})
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		if my.args.errorLogger != nil {
			my.args.errorLogger("Failed to write batch messages:", err)
		}
		return err
	}

	return nil
}

func (my *StreamWriter) cleanOldMessages(ctx context.Context) {
	// 计算截止时间
	cutoff := time.Now().Add(-my.args.maxAge)

	// 将时间转换为Redis Stream的ID格式 (timestamp-sequence)
	cutoffID := formatStreamID(cutoff.UnixMilli())

	// 删除旧消息
	err := my.client.XTrimMinID(ctx, my.streamKey, cutoffID).Err()
	if err != nil && my.args.errorLogger != nil {
		my.args.errorLogger("Failed to clean old messages:", err)
	}
}

// formatStreamID 将毫秒时间戳格式化为Redis Stream ID格式 (timestamp-sequence)
func formatStreamID(timestampMs int64) string {
	return fmt.Sprintf("%d-0", timestampMs)
}

func (my *StreamWriter) Close() error {
	return my.wc.Close(nil)
}
