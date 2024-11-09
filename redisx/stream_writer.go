package redisx

/********************************************************************
created:    2021-06-29
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type StreamWriter struct {
	streamKey string
}

func NewStreamWriter(streamKey string) *StreamWriter {
	var my = &StreamWriter{
		streamKey: streamKey,
	}

	return my
}
