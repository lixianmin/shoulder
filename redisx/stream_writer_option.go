package redisx

import "time"

/********************************************************************
created:    2024-11-09
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type streamWriterArguments struct {
	maxLen int64         // stream最大长度
	maxAge time.Duration // 消息最大保存时间
}

type StreamWriterOption func(*streamWriterArguments)

func WithMaxLen(maxLen int64) StreamWriterOption {
	return func(args *streamWriterArguments) {
		args.maxLen = maxLen
	}
}

func WithMaxAge(duration time.Duration) StreamWriterOption {
	return func(args *streamWriterArguments) {
		args.maxAge = duration
	}
}
