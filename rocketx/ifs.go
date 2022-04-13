package rocketx

/********************************************************************
created:    2022-04-08
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type Message interface {
	GetShardingKey() string // 当使用uid作为sharding key的时候, 同一个用户的消息将发送到同一个queue中, 这样在该用户身上可做到顺序一致性. 如果一直返回空串, 则会fallback到random路由
}
