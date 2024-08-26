package redisx

import (
	"context"
	"errors"
	"strconv"

	"github.com/lixianmin/got/convert"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
)

/********************************************************************
created:    2021-05-05
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

func execPipeline(ctx context.Context, pipe redis.Pipeliner) error {
	if cmds, err := pipe.Exec(ctx); err != nil {
		for _, cmd := range cmds {
			if cmd.Err() != nil {
				return errors.New(formatCmd(cmd))
			}
		}
		return err
	}

	return nil
}

func formatCmd(cmd redis.Cmder) string {
	var buffer = make([]byte, 0, 8)

	buffer = append(buffer, "err= "...)
	buffer = append(buffer, cmd.Err().Error()...)
	buffer = append(buffer, ", args=["...)

	var args = cmd.Args()
	var size = len(args)
	for i, arg := range args {
		buffer = strconv.AppendQuote(buffer, cast.ToString(arg))
		if i+1 < size {
			buffer = append(buffer, ", "...)
		}
	}

	buffer = append(buffer, ']')
	return convert.String(buffer)
}
