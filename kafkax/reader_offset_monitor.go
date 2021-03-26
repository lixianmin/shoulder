package kafkax

import (
	"github.com/lixianmin/got/timex"
	"github.com/lixianmin/logo"
	"github.com/segmentio/kafka-go"
)

/********************************************************************
created:    2021-03-11
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

// 监控消息丢失
type readerOffsetMonitor struct {
	lastOffsets map[int]int64
}

func newReaderOffsetMonitor() *readerOffsetMonitor {
	var my = &readerOffsetMonitor{
		lastOffsets: make(map[int]int64, 4),
	}

	return my
}

func (my *readerOffsetMonitor) checkOffset(msg kafka.Message) {
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
