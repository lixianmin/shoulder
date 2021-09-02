package cronx

import "github.com/robfig/cron/v3"

/********************************************************************
created:    2020-09-02
author:     lixianmin

	// https://www.bejson.com/othertools/cron/
	// 每天10:30，干点啥
	_, _ = c.AddFunc("0 30 10 * * ?", func() {
		......
	})

Copyright (C) - All Rights Reserved
*********************************************************************/

func NewDefault() *cron.Cron {
	var my = cron.New(cron.WithSeconds(), cron.WithChain(cron.Recover(&Logger{})))
	return my
}
