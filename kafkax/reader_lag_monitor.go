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

	startStats   kafka.ReaderStats
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
				logo.JsonW("lagNum", stats.Lag, "lagTime", timex.FormatDuration(lagTime), "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset,
					"time", timex.FormatTime(msg.Time), "groupId", reader.Config().GroupID)
				my.state = monitorStateLagging
				my.eventCounter = 0

				my.startStats = stats
				my.startLagTime = now
				my.nextWarnTime = now.Add(warnInterval)
			}
		}
	} else if my.state == monitorStateLagging {
		if lagTime < my.lagLimit {
			my.eventCounter++
			if my.eventCounter > eventLimit { // 恢复正常
				logo.JsonW("normal", timex.FormatDuration(lagTime), "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset,
					"time", timex.FormatTime(msg.Time), "groupId", reader.Config().GroupID)
				my.state = monitorStateNormal
				my.eventCounter = 0
			}
		} else if now.After(my.nextWarnTime) { // 每间隔1分钟，报警一次
			var lasting = now.Sub(my.startLagTime)
			var stats = reader.Stats()
			var estimate = my.calculateEstimateTime(lasting, stats)

			logo.JsonW("lasting", timex.FormatDuration(lasting), "estimate", timex.FormatDuration(estimate), "lagNum", stats.Lag, "lagTime", timex.FormatDuration(lagTime),
				"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "time", timex.FormatTime(msg.Time), "groupId", reader.Config().GroupID)
			my.nextWarnTime = my.nextWarnTime.Add(warnInterval)
		}
	}
}

// 根据过去的处理速度，预估剩余处理时间
func (my *readerLagMonitor) calculateEstimateTime(lasting time.Duration, currentStats kafka.ReaderStats) time.Duration {
	var estimateTime time.Duration

	var currentOffset = currentStats.Offset
	var startOffset = my.startStats.Offset
	var lagNum = currentStats.Lag

	// 有的时候 startStats.Lag < currentStats.Lag ，是因为处理的太慢了，处理的还不如接收的消息多，越处理越慢
	// offset之差是真正处理的个数
	var processed = currentOffset - startOffset
	if processed > 0 {
		estimateTime = lasting * time.Duration(lagNum/processed)
	} else {
		logo.JsonW("processed", processed, "currentOffset", currentOffset, "startOffset", startOffset, "lagNum", lagNum)
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
