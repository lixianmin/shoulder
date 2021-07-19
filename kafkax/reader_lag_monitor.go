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
type readerLagMonitor struct {
	state        int
	lagLimit     time.Duration
	eventCounter int
	checkEvery   int // 每多少次检查一下

	startLagNum  int64
	startLagTime time.Time
	nextWarnTime time.Time
}

func newReaderLagMonitor(lagLimit time.Duration) *readerLagMonitor {
	var my = &readerLagMonitor{
		state:    monitorStateNormal,
		lagLimit: lagLimit,
	}

	return my
}

func (my *readerLagMonitor) checkConsumeLag(reader *kafka.Reader, msg kafka.Message) {
	if !my.needCheck() {
		return
	}

	const eventLimit = 5
	const warnInterval = time.Minute

	var now = time.Now()
	var lagTime = now.Sub(msg.Time)

	if my.state == monitorStateNormal {
		if lagTime > my.lagLimit {
			my.eventCounter++
			if my.eventCounter > eventLimit {
				var stats = reader.Stats()
				logo.JsonW("lagNum", stats.Lag, "lagTime", timex.FormatDuration(lagTime), "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "time", timex.FormatTime(msg.Time))
				my.state = monitorStateLagging
				my.eventCounter = 0

				my.startLagNum = stats.Lag
				my.startLagTime = now
				my.nextWarnTime = now.Add(warnInterval)
			}
		}
	} else if my.state == monitorStateLagging {
		if lagTime < my.lagLimit {
			my.eventCounter++
			if my.eventCounter > eventLimit { // 恢复正常
				logo.JsonW("normal", timex.FormatDuration(lagTime), "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "time", timex.FormatTime(msg.Time))
				my.state = monitorStateNormal
				my.eventCounter = 0
			}
		} else if now.After(my.nextWarnTime) { // 每间隔1分钟，报警一次
			var lasting = now.Sub(my.startLagTime)
			var stats = reader.Stats()
			var estimate = my.calculateEstimateTime(lasting, stats.Lag)

			logo.JsonW("lasting", timex.FormatDuration(lasting), "estimate", timex.FormatDuration(estimate), "lagNum", stats.Lag,
				"lagTime", timex.FormatDuration(lagTime), "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "time", timex.FormatTime(msg.Time))
			my.nextWarnTime = my.nextWarnTime.Add(warnInterval)
		}
	}
}

// 根据过去的处理速度，预估剩余处理时间
func (my *readerLagMonitor) calculateEstimateTime(lasting time.Duration, lagNum int64) time.Duration {
	var estimateTime = time.Duration(0)
	var processed = my.startLagNum - lagNum
	if processed > 0 {
		estimateTime = lasting * time.Duration(lagNum/processed)
	} else {
		my.startLagNum = lagNum // 有的时候processed不大于0，很奇怪，但不知道为啥
	}

	return estimateTime
}

func (my *readerLagMonitor) needCheck() bool {
	my.checkEvery++
	if my.checkEvery > 10 {
		my.checkEvery = 0
		return true
	}

	return false
}
