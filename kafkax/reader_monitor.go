package kafkax

import (
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
}

func (my *readerMonitor) checkConsumeLag(msg kafka.Message) {
	if !my.needCheck() {
		return
	}

	const eventLimit = 5
	var lag = time.Now().Sub(msg.Time)

	if my.state == MonitorStateNormal {
		if lag > my.lagLimit {
			my.eventCounter++
			if my.eventCounter > eventLimit {
				logo.JsonW("lastState", MonitorStateNormal, "nextState", MonitorStateLagging, "lag", lag.String())
				my.state = MonitorStateLagging
				my.eventCounter = 0
			}
		}
	} else if my.state == MonitorStateLagging {
		if lag < my.lagLimit {
			my.eventCounter++
			if my.eventCounter > eventLimit {
				logo.JsonW("lastState", MonitorStateLagging, "nextState", MonitorStateNormal, "lag", lag.String())
				my.state = MonitorStateNormal
				my.eventCounter = 0
			}
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
