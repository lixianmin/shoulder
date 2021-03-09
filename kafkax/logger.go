package kafkax

/********************************************************************
created:    2021-03-08
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type logger struct {
	PrintFunc func(first interface{}, args ...interface{})
}

func (my *logger) Printf(format string, args ...interface{}) {
	my.PrintFunc(format, args...)
}
