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

// 监控消息流失
type readerOffsetMonitor struct {
	lastOffset int64
}

func (my *readerOffsetMonitor) checkOffset(msg kafka.Message) {
	if my.lastOffset > 0 {
		var lostMessageCount = msg.Offset - my.lastOffset - 1
		if lostMessageCount > 0 {
			logo.JsonW("lostMessageCount", lostMessageCount, "lastOffset", my.lastOffset, "offset", msg.Offset, "topic", msg.Topic, "partition", msg.Partition, "time", timex.FormatTime(msg.Time))
		}
	}

	my.lastOffset = msg.Offset
}
