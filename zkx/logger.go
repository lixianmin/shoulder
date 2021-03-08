package zkx

import "github.com/lixianmin/logo"

/********************************************************************
created:    2021-03-08
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type Logger struct {
}

func (my *Logger) Printf(format string, args ...interface{}) {
	logo.GetLogger().Info(format, args...)
}
