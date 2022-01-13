package internal

import (
	"github.com/lixianmin/got/timex"
	"github.com/lixianmin/logo"
	"github.com/segmentio/kafka-go"
)

/********************************************************************
created:    2021-03-11
author:     lixianmin

监控消息丢失：基本假设是消息的offset是一个接着一个的，两个紧邻的消息之间的offset差
是1。但是，对于执行了log compact的topic，在重新消费其历史数据的时候，很多消息因为
compact被去除了，所以，两个紧邻的消息之间的offset差就不是1了，会产生误报。

Copyright (C) - All Rights Reserved
*********************************************************************/

type ReaderOffsetMonitor struct {
	lastOffsets map[int]int64
}

func NewReaderOffsetMonitor() *ReaderOffsetMonitor {
	var my = &ReaderOffsetMonitor{
		lastOffsets: make(map[int]int64, 4),
	}

	return my
}

func (my *ReaderOffsetMonitor) checkOffset(msg kafka.Message) {
	var partition = msg.Partition
	var lastOffset = my.lastOffsets[partition]

	if lastOffset > 0 {
		var lostMessageCount = msg.Offset - lastOffset - 1
		if lostMessageCount > 0 {
			logo.JsonW("lostMessageCount", lostMessageCount, "lastOffset", lastOffset, "offset", msg.Offset, "topic", msg.Topic, "partition", partition, "time", timex.FormatTime(msg.Time))
		}
	}

	my.lastOffsets[partition] = msg.Offset
}
