package cronx

import "github.com/robfig/cron/v3"

/********************************************************************
created:    2020-09-02
author:     lixianmin

	// https://www.bejson.com/othertools/cron/
	_, _ = c.AddFunc("0 30 10 * * ?", func() {
		......
	})

	0 */10 * * * *  -> 每10分钟执行一次
    0 30 10 * * *   -> 每天早上10:30

Copyright (C) - All Rights Reserved
*********************************************************************/

func NewDefault() *cron.Cron {
	var my = cron.New(cron.WithSeconds(), cron.WithChain(cron.Recover(&Logger{})))
	return my
}
