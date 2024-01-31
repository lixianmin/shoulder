package cronx

import "github.com/robfig/cron/v3"

/********************************************************************
created:    2020-09-02
author:     lixianmin

	// 可参考其中的Linux/Java(Spring)
	https://tool.lu/crontab/

	_, _ = c.AddFunc("0 30 10 * * ?", func() {

	})

Copyright (C) - All Rights Reserved
*********************************************************************/

//////////////////////////////////////////////////////////////////////
// 以下几个例子都是测试通过的:
//
// */10 * * * *  -> 每10分钟执行一次
// 30 10 * * *   -> 每天早上10:30
//////////////////////////////////////////////////////////////////////

func NewLinux() *cron.Cron {
	var my = cron.New(cron.WithChain(cron.Recover(&Logger{})))
	return my
}

//////////////////////////////////////////////////////////////////////
// 以下几个例子都是测试通过的:
//
// 0 */10 * * * *  -> 每10分钟执行一次
// TZ=Asia/Shanghai 0 0 6 * * *   -> 每天早上06:00:00
//////////////////////////////////////////////////////////////////////

func NewJava() *cron.Cron {
	var my = cron.New(cron.WithSeconds(), cron.WithChain(cron.Recover(&Logger{})))
	return my
}
