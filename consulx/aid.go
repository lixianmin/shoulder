package consulx

/********************************************************************
created:    2019-02-01
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

import (
	"fmt"
	"github.com/hashicorp/consul/api"
	"github.com/lixianmin/shoulder/cachex"
	"math/rand"
	"strings"
	"time"
)

type Aid struct {
	address                        string // consul服务器地址，含端口："127.0.0.1:8500"
	Tag                            []string
	DeregisterCriticalServiceAfter time.Duration
	CheckInterval                  time.Duration

	cache *cachex.Cache
}

func NewAid(consulAddress string) *Aid {
	var aid = &Aid{
		address:                        consulAddress,
		Tag:                            []string{},
		DeregisterCriticalServiceAfter: time.Duration(1) * time.Minute,
		CheckInterval:                  time.Duration(10) * time.Second,
		cache:                          cachex.NewCache(10000, 1024*1024),
	}

	return aid
}

func (aid *Aid) RegisterService(name string, port int) error {
	var config = api.DefaultConfig()
	config.Address = aid.address

	var client, err = api.NewClient(config)
	if err != nil {
		return err
	}

	var agent = client.Agent()

	var hostIP = GetLocalIp()
	var hostIPArgument = strings.Replace(hostIP, ".", "-", -1)
	var nodeID = fmt.Sprintf("%v-%v-%v", name, hostIPArgument, port)
	var httpHealthUrl = fmt.Sprintf("http://%v:%v/health", hostIP, port)
	var reg = &api.AgentServiceRegistration{
		ID:      nodeID,  // 服务节点的名称
		Name:    name,    // 服务名称
		Tags:    aid.Tag, // tag，可以为空
		Port:    port,    // 服务端口
		Address: hostIP,  // 服务 hostIP
		Check: &api.AgentServiceCheck{ // 健康检查
			Interval:                       aid.CheckInterval.String(), // 健康检查间隔
			HTTP:                           httpHealthUrl,
			DeregisterCriticalServiceAfter: aid.DeregisterCriticalServiceAfter.String(), // 注销时间，相当于过期时间
		},
	}

	if err := agent.ServiceRegister(reg); err != nil {
		return err
	}

	return nil
}

func (aid *Aid) GetService(name string) (*api.AgentService, error) {

	var item = aid.cache.Load(name, func() (interface{}, time.Duration) {
		var config = api.DefaultConfig()
		config.Address = aid.address

		var client, err = api.NewClient(config)
		if err != nil {
			return err, time.Second
		}

		var health = client.Health()
		entries, _, err := health.Service(name, "", true, nil)
		if err != nil {
			return err, time.Second
		}

		var services = make([]*api.AgentService, len(entries))[:0]
		for i := range entries {
			var entry = entries[i]
			services = append(services, entry.Service)
		}

		return services, 5 * time.Second
	})

	switch item := item.(type) {
	case error:
		return nil, item
	case []*api.AgentService:
		if len(item) > 0 {
			var service = getRandomService(item)
			return service, nil
		}
	}

	return nil, fmt.Errorf("found no service with name=%s", name)
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
