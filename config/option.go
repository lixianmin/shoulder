package config

/********************************************************************
created:    2021-02-01
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type arguments struct {
	envPrefix string
}

type Option func(*arguments)

func createArguments(options []Option) arguments {
	var args = arguments{
	}

	for _, opt := range options {
		opt(&args)
	}

	return args
}

func WithViperEnvPrefix(prefix string) Option {
	return func(args *arguments) {
		args.envPrefix = prefix
	}
}
