package rocketx

import (
	"os"
	"path/filepath"
)

/********************************************************************
created:    2020-06-13
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

var serviceName string

func init() {
	serviceName = filepath.Base(os.Args[0])
}

func ServiceName() string {
	return serviceName
}
