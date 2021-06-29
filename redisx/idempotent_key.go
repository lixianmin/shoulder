package redisx

import "strconv"

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
	var keys = append([]string{my.Key, strconv.FormatInt(int64(my.Partition), 10), strconv.FormatInt(my.Offset, 10)}, args...)
	return keys
}