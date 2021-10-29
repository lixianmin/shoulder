package gprcx

import "context"
import "google.golang.org/grpc/health/grpc_health_v1"

/********************************************************************
created:    2021-10-29
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type rpcHealth struct {
}

// Check 实现健康检查接口，这里直接返回健康状态，这里也可以有更复杂的健康检查策略，比如根据服务器负载来返回
func (h *rpcHealth) Check(ctx context.Context, request *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	var response = &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	}

	return response, nil
}

func (h *rpcHealth) Watch(request *grpc_health_v1.HealthCheckRequest, watch grpc_health_v1.Health_WatchServer) error {
	return nil
}
