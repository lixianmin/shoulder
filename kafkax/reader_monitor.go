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

	lagTime      time.Time
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
	var lag = now.Sub(msg.Time)

	if my.state == monitorStateNormal {
		if lag > my.lagLimit {
			my.eventCounter++
			if my.eventCounter > eventLimit {
				logo.JsonW("lastState", "normal", "nextState", "lagging", "lag", lag.String(), "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "time", msg.Time.Format(timex.Layout))
				my.state = monitorStateLagging
				my.eventCounter = 0

				my.lagTime = now
				my.nextWarnTime = now.Add(warnInterval)
			}
		}
	} else if my.state == monitorStateLagging {
		if lag < my.lagLimit {
			my.eventCounter++
			if my.eventCounter > eventLimit {
				logo.JsonW("lastState", "lagging", "nextState", "normal", "lag", lag.String(), "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "time", msg.Time.Format(timex.Layout))
				my.state = monitorStateNormal
				my.eventCounter = 0
			}
		} else if now.After(my.nextWarnTime) {
			// 每间隔1分钟，报警一次
			logo.JsonW("state", "normal", "laggingTime", now.Sub(my.lagTime).String())
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
