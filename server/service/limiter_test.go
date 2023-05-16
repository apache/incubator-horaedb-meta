// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package service

import (
	"testing"
	"time"

	"github.com/CeresDB/ceresmeta/server/config"
	"github.com/stretchr/testify/require"
)

const (
	defaultGrpcOperationRate0  = 10
	defaultGrpcOperationBurst0 = 1000
	defaultGrpcOperationRate1  = 100
	defaultGrpcOperationBurst1 = 100
)

func TestFlowLimiter(t *testing.T) {
	re := require.New(t)
	flowLimiter := NewFlowLimiter(config.LimiterConfig{Rate: defaultGrpcOperationRate0, Burst: defaultGrpcOperationBurst0})

	for i := 0; i < defaultGrpcOperationBurst0; i++ {
		flag := flowLimiter.Allow()
		re.Equal(true, flag)
	}

	flag := flowLimiter.Allow()
	re.Equal(false, flag)

	time.Sleep(time.Second)
	for i := 0; i < defaultGrpcOperationRate0; i++ {
		flag := flowLimiter.Allow()
		re.Equal(true, flag)
	}

	flag = flowLimiter.Allow()
	re.Equal(false, flag)

	value := flowLimiter.GetThreshold()
	re.Equal(defaultGrpcOperationBurst0, value)

	flowLimiter.UpdateLimiter(config.LimiterConfig{Rate: defaultGrpcOperationRate1, Burst: defaultGrpcOperationBurst1})
	value = flowLimiter.GetThreshold()
	re.Equal(defaultGrpcOperationBurst1, value)

	time.Sleep(time.Second * 2)
	for i := 0; i < defaultGrpcOperationBurst1; i++ {
		flag := flowLimiter.Allow()
		re.Equal(true, flag)
	}

	flag = flowLimiter.Allow()
	re.Equal(false, flag)

	time.Sleep(time.Second)
	for i := 0; i < defaultGrpcOperationRate1; i++ {
		flag := flowLimiter.Allow()
		re.Equal(true, flag)
	}

	flag = flowLimiter.Allow()
	re.Equal(false, flag)
}
