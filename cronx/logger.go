package cronx

import (
	"github.com/lixianmin/logo"
)

/********************************************************************
created:    2020-09-02
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type Logger struct {
}

// Info logs routine messages about cron's operation.
func (my *Logger) Info(msg string, keysAndValues ...interface{}) {
	logo.GetLogger().Info(msg, keysAndValues...)
}

// Error logs an error condition.
func (my *Logger) Error(err error, msg string, keysAndValues ...interface{}) {
	var format = msg
	if err != nil {
		format = "err=[" + err.Error() + "], " + msg
	}

	logo.GetLogger().Error(format, keysAndValues...)
}
