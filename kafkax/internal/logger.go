package internal

/********************************************************************
created:    2021-03-08
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type Logger struct {
	PrintFunc func(first interface{}, args ...interface{})
}

func (my *Logger) Printf(format string, args ...interface{}) {
	my.PrintFunc(format, args...)
}
