package grpcx

import (
	"context"
	_ "github.com/mbobakov/grpc-consul-resolver"
	"google.golang.org/grpc"
)

/********************************************************************
created:    2021-10-29
author:     lixianmin

https://github.com/mbobakov/grpc-consul-resolver

consul://[user:password@]127.0.0.127:8555/my-service?[healthy=]&[wait=]&[near=]&[insecure=]&[limit=]&[tag=]&[token=]

// conn, err := grpc.DialContext(ctx, my.serviceUrl, grpc.WithInsecure(), grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy": "round_robin"}`))

Copyright (C) - All Rights Reserved
*********************************************************************/

func DialByConsul(ctx context.Context, target string, opts ...grpc.DialOption) (conn *grpc.ClientConn, err error) {
	return grpc.DialContext(ctx, target, opts...)
}