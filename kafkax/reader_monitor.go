package kafkax

import (
	"github.com/lixianmin/got/timex"
	"github.com/lixianmin/logo"
	"github.com/segmentio/kafka-go"
	"time"
)

/********************************************************************
created:    2021-03-11
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

// 监控消费延迟
type readerMonitor struct {
	state        int
	lagLimit     time.Duration
	eventCounter int
	checkEvery   int // 每多少次检查一下

	startLagTime time.Time
	nextWarnTime time.Time
}

func newReaderMonitory(lagLimit time.Duration) *readerMonitor {
	var my = &readerMonitor{
		state:    monitorStateNormal,
		lagLimit: lagLimit,
	}

	return my
}

func (my *readerMonitor) checkConsumeLag(msg kafka.Message) {
	if !my.needCheck() {
		return
	}

	const eventLimit = 5
	const warnInterval = time.Minute

	var now = time.Now()
	var lagged = now.Sub(msg.Time)

	if my.state == monitorStateNormal {
		if lagged > my.lagLimit {
			my.eventCounter++
			if my.eventCounter > eventLimit {
				logo.JsonW("lagged", timex.FormatDuration(lagged), "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "time", timex.FormatTime(msg.Time))
				my.state = monitorStateLagging
				my.eventCounter = 0

				my.startLagTime = now
				my.nextWarnTime = now.Add(warnInterval)
			}
		}
	} else if my.state == monitorStateLagging {
		if lagged < my.lagLimit {
			my.eventCounter++
			if my.eventCounter > eventLimit { // 恢复正常
				logo.JsonW("normal", timex.FormatDuration(lagged), "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "time", timex.FormatTime(msg.Time))
				my.state = monitorStateNormal
				my.eventCounter = 0
			}
		} else if now.After(my.nextWarnTime) { // 每间隔1分钟，报警一次
			var lasting = now.Sub(my.startLagTime)
			logo.JsonW("lasting", timex.FormatDuration(lasting), "lagged", timex.FormatDuration(lagged), "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "time", timex.FormatTime(msg.Time))
			my.nextWarnTime = my.nextWarnTime.Add(warnInterval)
		}
	}
}

func (my *readerMonitor) needCheck() bool {
	my.checkEvery++
	if my.checkEvery > 10 {
		my.checkEvery = 0
		return true
	}

	return false
}
