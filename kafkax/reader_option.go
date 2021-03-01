package kafkax

import (
	"os"
	"path/filepath"
)

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

func createReaderArguments(options []ReaderOption) readerArguments {
	var serviceName = filepath.Base(os.Args[0])
	var args = readerArguments{
		groupId:  serviceName,
		minBytes: 10e3, // 10KB
		maxBytes: 10e6, // 10MB
	}

	for _, opt := range options {
		opt(&args)
	}

	return args
}

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
