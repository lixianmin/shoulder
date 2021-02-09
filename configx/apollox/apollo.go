package apollox

import (
	"fmt"
	"github.com/lixianmin/got/loom"
	"github.com/lixianmin/logo"
	"github.com/shima-park/agollo"
	remote "github.com/shima-park/agollo/viper-remote"
	"github.com/spf13/viper"
	"sync"
)

/********************************************************************
created:    2021-02-01
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type Apollo struct {
	serverUrl string
	appId     string
	namespace string
	ago       agollo.Agollo
	handlers  sync.Map
}

func NewApollo(serverUrl string, appId string, namespace string) *Apollo {
	ago, err := agollo.New(serverUrl, appId, agollo.DefaultNamespace(namespace), agollo.AutoFetchOnCacheMiss())
	if err != nil {
		logo.JsonE("err", err)
		return nil
	}

	var my = &Apollo{
		serverUrl: serverUrl,
		appId:     appId,
		namespace: namespace,
		ago:       ago,
	}

	// 这两个方法不能在goroutine里调用，会引发data race
	var errorChan, watchChan = ago.Start(), ago.Watch()
	go my.goLoop(errorChan, watchChan)

	return my
}

func (my *Apollo) FillViper(vip *viper.Viper) error {
	if vip == nil {
		return fmt.Errorf("vip is nil")
	}

	remote.SetAppID(my.appId)

	vip.SetConfigType("prop") // 这个需要设置，表明是 properties 格式

	// 这个AddRemoteProvider()虽然可以设置多个，但只有一个起作用，可能是为了兼容 etcd 这种远程服务器
	// 所以，application这个namespace不用了，直接使用crab.dsn，由每个appId对应的项目以覆盖的形式创建所有的配置
	var err = vip.AddRemoteProvider("apollo", my.serverUrl, my.namespace)
	if err != nil {
		return err
	}

	err = vip.ReadRemoteConfig()
	if err != nil {
		return err
	}

	// todo vip的WatchRemoteConfigOnChannel()会引发data race
	//err = vip.WatchRemoteConfigOnChannel()
	//if err != nil {
	//	return err
	//}

	return nil

}

func (my *Apollo) Watch(key string, callback func(change agollo.Change)) {
	if my.ago == nil {
		logo.JsonE("err", "my.ago is nil")
		return
	}

	var change = agollo.Change{
		Type:  agollo.ChangeTypeAdd,
		Key:   key,
		Value: my.ago.Get(key, agollo.WithNamespace(my.namespace)),
	}

	callback(change)
	my.handlers.Store(key, callback)
}

func (my *Apollo) goLoop(errorChan <-chan *agollo.LongPollerError, watchChan <-chan *agollo.ApolloResponse) {
	defer loom.DumpIfPanic()

	for {
		select {
		case err := <-errorChan:
			logo.JsonW("err", err)
		case event := <-watchChan:
			if event.Error != nil {
				logo.JsonW("err", event.Error)
				continue
			}

			for _, change := range event.Changes {
				var key = change.Key
				if v, ok := my.handlers.Load(key); ok {
					var handler = v.(func(change agollo.Change))
					handler(change)
				}
			}
		}
	}
}

func (my *Apollo) GetAgollo() agollo.Agollo {
	return my.ago
}

func (my *Apollo) Get(key string) string{
	return my.ago.Get(key, agollo.WithNamespace(my.namespace))
}
