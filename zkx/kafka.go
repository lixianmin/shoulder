package zkx

import (
	"github.com/wvanbergen/kazoo-go"
)

/********************************************************************
created:    2021-02-23
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

func RequestKafkaBrokers(zkAddress string) ([]string, error) {
	var config = kazoo.NewConfig()
	var zk, err = kazoo.NewKazooFromConnectionString(zkAddress, config)
	if err != nil {
		return nil, err
	}

	brokerList, err := zk.BrokerList()
	if err != nil {
		return nil, err
	}

	return brokerList, nil
}
