package kafkax

/********************************************************************
created:    2021-03-01
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type readerArguments struct {
	groupId  string
	minBytes int
	maxBytes int
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
