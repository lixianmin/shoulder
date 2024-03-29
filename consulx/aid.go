package consulx

/********************************************************************
created:    2019-02-01
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

import (
	"fmt"
	"github.com/hashicorp/consul/api"
	"github.com/lixianmin/got/cachex"
	"math/rand"
	"time"
)

type Aid struct {
	addressList []string // consul服务器地址列表，含端口："127.0.0.1:8500"
	client      *api.Client
	errClient   error
	cache       cachex.Cache
}

func NewAid(addressList []string) *Aid {
	var aid = &Aid{
		addressList: addressList,
		cache:       cachex.NewCache(cachex.WithExpire(10*time.Second, 1*time.Second)),
	}

	aid.client, aid.errClient = createConsulClient(addressList)
	return aid
}

func (aid *Aid) RegisterService(name string, port int, options ...RegisterOption) error {
	if aid.errClient != nil {
		return aid.errClient
	}

	var args = registerArguments{
		tags:                           []string{},
		checkInterval:                  10 * time.Second,
		deregisterCriticalServiceAfter: 1 * time.Minute,
		healthCheck:                    HealthCheckHTTP,
	}

	for _, opt := range options {
		opt(&args)
	}

	var agent = aid.client.Agent()

	var hostIp = GetLocalIp()
	//var hostIPArgument = strings.Replace(hostIp, ".", "-", -1)
	//var nodeId = fmt.Sprintf("%v-%v-%v", name, hostIPArgument, port)
	var nodeId = fmt.Sprintf("%v-%v:%v", name, hostIp, port)

	// 设置默认的 health check
	var healthCheckHttp, healthCheckGrpc = "", ""
	switch args.healthCheck {
	case HealthCheckHTTP:
		healthCheckHttp = fmt.Sprintf("http://%s:%d/health", hostIp, port)
	case HealthCheckGRPC:
		healthCheckGrpc = fmt.Sprintf("%s:%d/%s", hostIp, port, name)
	}

	var reg = &api.AgentServiceRegistration{
		ID:      nodeId,    // 服务节点的名称
		Name:    name,      // 服务名称
		Tags:    args.tags, // tag，可以为空
		Port:    port,      // 服务端口
		Address: hostIp,    // 服务 hostIp
		Check: &api.AgentServiceCheck{ // 健康检查
			Interval:                       args.checkInterval.String(), // 健康检查间隔
			HTTP:                           healthCheckHttp,
			GRPC:                           healthCheckGrpc,
			DeregisterCriticalServiceAfter: args.deregisterCriticalServiceAfter.String(), // 注销时间，相当于过期时间
		},
	}

	if err := agent.ServiceRegister(reg); err != nil {
		return err
	}

	return nil
}

func (aid *Aid) GetService(name string) (*api.AgentService, error) {
	var item, err = aid.cache.Load(name, func(key interface{}) (interface{}, error) {
		if aid.errClient != nil {
			return nil, aid.errClient
		}

		var health = aid.client.Health()
		entries, _, err := health.Service(name, "", true, nil)
		if err != nil {
			return nil, err
		}

		var services = make([]*api.AgentService, len(entries))[:0]
		for i := range entries {
			var entry = entries[i]
			services = append(services, entry.Service)
		}

		return services, nil
	}).Get2()
	if err != nil {
		return nil, err
	}

	if services, ok := item.([]*api.AgentService); ok {
		var service = getRandomService(services)
		return service, nil
	}

	return nil, fmt.Errorf("found no service with name=%s", name)
}

func (aid *Aid) GetClient() *api.Client {
	return aid.client
}

func createConsulClient(addressList []string) (*api.Client, error) {
	var list = addressList
	rand.Shuffle(len(list), func(i, j int) {
		list[i], list[j] = list[j], list[i]
	})

	var config = api.DefaultConfig()

	var client *api.Client
	var err error

	for i := 0; i < len(list); i++ {
		config.Address = list[i]
		client, err = api.NewClient(config)
		if err == nil {
			return client, nil
		}
	}

	return nil, err
}

func getRandomService(services []*api.AgentService) *api.AgentService {
	var count = len(services)
	if count > 0 {
		var index = rand.Intn(count)
		var service = services[index]
		return service
	}

	return nil
}
