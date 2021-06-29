package redisx

/********************************************************************
created:    2021-06-29
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type IdempotentKey struct {
	Key       string
	Partition int
	Offset    int64
}

func (my *IdempotentKey) getKeys(args ...string) []string {
	var keys = append([]string{my.Key, itoa(int64(my.Partition)), itoa(my.Offset)}, args...)
	return keys
}
