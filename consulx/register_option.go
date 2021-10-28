package consulx

import (
	"time"
)

/********************************************************************
created:    2021-03-22
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type registerArguments struct {
	tags                           []string
	checkInterval                  time.Duration
	deregisterCriticalServiceAfter time.Duration
	healthCheck                    HealthCheck
}

type HealthCheck struct {
	HTTP string
	GRPC string
}

type RegisterOption func(*registerArguments)

func WithTags(tags ...string) RegisterOption {
	return func(args *registerArguments) {
		args.tags = tags
	}
}

func WithCheckInterval(interval time.Duration) RegisterOption {
	return func(args *registerArguments) {
		args.checkInterval = interval
	}
}

func WithDeregisterCriticalServiceAfter(interval time.Duration) RegisterOption {
	return func(args *registerArguments) {
		args.deregisterCriticalServiceAfter = interval
	}
}

func WithHealthCheck(check HealthCheck) RegisterOption {
	return func(args *registerArguments) {
		args.healthCheck = check
	}
}
