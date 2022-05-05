package redisx

import "time"

/********************************************************************
created:    2021-05-25
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type FlushAid struct {
	flushSize     int
	flushInterval time.Duration
	nextFlushTime time.Time
}

func NewFlushAid(flushSize int, flushInterval time.Duration) *FlushAid {
	var my = &FlushAid{
		flushSize:     flushSize,
		flushInterval: flushInterval,
		nextFlushTime: time.Now(),
	}

	return my
}

func (my *FlushAid) CanFlush(currentSize int) bool {
	if currentSize >= my.flushSize {
		return true
	}

	if currentSize > 0 {
		var now = time.Now()
		if now.After(my.nextFlushTime) {
			my.nextFlushTime = now.Add(my.flushInterval)
			return true
		}
	}

	return false
}
