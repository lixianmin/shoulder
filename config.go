package fig

import (
	"fmt"
	"github.com/spf13/viper"
	"os"
	"strings"
)

/********************************************************************
created:    2021-02-01
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type Config struct {
	vip *viper.Viper
}

func New(configPath string, options ...Option) *Config {
	var args = createArguments(options)
	var vip = createViper(configPath, args)

	var my = &Config{
		vip: vip,
	}

	return my
}

func (my *Config) GetViper() *viper.Viper {
	return my.vip
}

func (my *Config) Get(key string) interface{} {
	return my.vip.Get(key)
}

func (my *Config) GetString(key string) string {
	return my.vip.GetString(key)
}

func (my *Config) GetInt(key string) int {
	return my.vip.GetInt(key)
}

func createViper(configPath string, args arguments) *viper.Viper {
	var vip = viper.New()
	vip.AddConfigPath(configPath)
	vip.SetEnvPrefix(args.envPrefix)

	var env, ok = os.LookupEnv("ENV")
	if !ok {
		env = "DEV"
	}

	var configName = "app_" + strings.ToLower(env)
	vip.SetConfigName(configName)

	var err = vip.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("can not load local config file, err=%q", err))
	}

	return vip
}
