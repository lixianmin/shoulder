package kafkax

import "time"

/********************************************************************
created:    2021-03-01
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type readerArguments struct {
	groupId         string
	minBytes        int
	maxBytes        int
	startOffset     int64
	messageChanSize int
	monitorLag      time.Duration
}

type ReaderOption func(*readerArguments)

func WithGroupId(groupId string) ReaderOption {
	return func(args *readerArguments) {
		args.groupId = groupId
	}
}

func WithMinBytes(minBytes int) ReaderOption {
	return func(args *readerArguments) {
		args.minBytes = minBytes
	}
}

func WithMaxBytes(maxBytes int) ReaderOption {
	return func(args *readerArguments) {
		args.maxBytes = maxBytes
	}
}

func WithStartOffset(startOffset int64) ReaderOption {
	return func(args *readerArguments) {
		args.startOffset = startOffset
	}
}

func WithMessageChanSize(size int) ReaderOption {
	return func(args *readerArguments) {
		if size > 0 {
			args.messageChanSize = size
		}
	}
}

func WithMonitorLag(lag time.Duration) ReaderOption {
	return func(args *readerArguments) {
		if lag > 0 {
			args.monitorLag = lag
		}
	}
}
