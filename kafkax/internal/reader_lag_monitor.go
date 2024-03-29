package internal

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

// ReaderLagMonitor 监控消费延迟
type ReaderLagMonitor struct {
	state        int
	lagLimit     time.Duration
	eventCounter int
	checkEvery   int // 每多少次检查一下

	startStats   kafka.ReaderStats
	startLagTime time.Time
	nextWarnTime time.Time
}

func NewReaderLagMonitor(lagLimit time.Duration) *ReaderLagMonitor {
	var my = &ReaderLagMonitor{
		state:    monitorStateNormal,
		lagLimit: lagLimit,
	}

	return my
}

func (my *ReaderLagMonitor) CheckConsumeLag(reader *kafka.Reader, msg kafka.Message) {
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

				my.resetStartStats(stats)
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
			var currentStats = reader.Stats()
			var estimate = my.calculateEstimateTime(lasting, currentStats)

			logo.JsonW("lasting", timex.FormatDuration(lasting), "estimate", timex.FormatDuration(estimate), "lagNum", currentStats.Lag, "lagTime", timex.FormatDuration(lagTime),
				"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "time", timex.FormatTime(msg.Time), "groupId", reader.Config().GroupID)
			my.nextWarnTime = my.nextWarnTime.Add(warnInterval)
		}
	}
}

// 根据过去的处理速度，预估剩余处理时间
func (my *ReaderLagMonitor) calculateEstimateTime(lasting time.Duration, currentStats kafka.ReaderStats) time.Duration {
	var estimate time.Duration

	var currentOffset = currentStats.Offset
	var startOffset = my.startStats.Offset
	var lagNum = currentStats.Lag

	// 有的时候 startStats.Lag < currentStats.Lag ，是因为处理的太慢了，处理的还不如接收的消息多，越处理越慢
	// offset之差是真正处理的个数
	var processed = currentOffset - startOffset
	if processed > 0 {
		estimate = lasting * time.Duration(lagNum/processed)
	} else {
		logo.JsonW("title", "offset回退了", "processed", processed, "currentOffset", currentOffset, "startOffset", startOffset, "lagNum", lagNum)

		// 虽然不是很清楚是什么原因导致的，但的确发现有些情况下currentOffset比startOffset还要小的情况，相当于是offset回溯了
		my.resetStartStats(currentStats)
	}

	// 现在有两个bug：
	// 1. lasting与estimate相同
	// 2. estimate一直为0.000s
	logo.JsonI("processed", processed, "currentOffset", currentOffset, "startOffset", startOffset, "lasting", timex.FormatDuration(lasting),
		"estimate", timex.FormatDuration(estimate), "lagNum", lagNum)
	return estimate
}

func (my *ReaderLagMonitor) resetStartStats(stats kafka.ReaderStats) {
	my.startStats = stats
	my.startLagTime = time.Now()
}

func (my *ReaderLagMonitor) needCheck() bool {
	my.checkEvery++
	if my.checkEvery > 10 {
		my.checkEvery = 0
		return true
	}

	return false
}
