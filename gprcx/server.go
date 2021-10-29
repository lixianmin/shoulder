package gprcx

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

/********************************************************************
created:    2021-10-29
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

func NewServer(serviceRegister func(*grpc.Server)) *grpc.Server {
	if serviceRegister == nil {
		panic("serviceRegister should not be nil")
	}

	// 创建RPC服务容器
	var server = grpc.NewServer()

	// 注册业务实现，将服务绑定到RPC服务容器上
	grpc_health_v1.RegisterHealthServer(server, &rpcHealth{})

	// 注册服务
	serviceRegister(server)

	// 注册反射服务，这个服务是CLI使用的，跟服务本身没有关系
	reflection.Register(server)
	return server
}
