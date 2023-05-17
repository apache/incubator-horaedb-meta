// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package service

import (
	"testing"
	"time"

	"github.com/CeresDB/ceresmeta/server/config"
	"github.com/stretchr/testify/require"
)

const (
	defaultInitialLimiterRate0     = 10
	defaultInitialLimiterCapacity0 = 1000
	defaultEnableLimiter           = true
	defaultInitialLimiterRate1     = 100
	defaultInitialLimiterCapacity1 = 100
)

func TestFlowLimiter(t *testing.T) {
	re := require.New(t)
	flowLimiter := NewFlowLimiter(config.LimiterConfig{TokenBucketFillRate: defaultInitialLimiterRate0, TokenBucketBurstEventCapacity: defaultInitialLimiterCapacity0, Enable: defaultEnableLimiter})

	for i := 0; i < defaultInitialLimiterCapacity0; i++ {
		flag := flowLimiter.Allow()
		re.Equal(true, flag)
	}

	flag := flowLimiter.Allow()
	re.Equal(false, flag)

	time.Sleep(time.Second)
	for i := 0; i < defaultInitialLimiterRate0; i++ {
		flag := flowLimiter.Allow()
		re.Equal(true, flag)
	}

	flag = flowLimiter.Allow()
	re.Equal(false, flag)

	value := flowLimiter.GetThreshold()
	re.Equal(defaultInitialLimiterCapacity0, value)

	err := flowLimiter.UpdateLimiter(config.LimiterConfig{
		TokenBucketFillRate:           defaultInitialLimiterRate1,
		TokenBucketBurstEventCapacity: defaultInitialLimiterCapacity1,
		Enable:                        defaultEnableLimiter,
	})
	re.NoError(err)
	value = flowLimiter.GetThreshold()
	re.Equal(defaultInitialLimiterCapacity1, value)

	time.Sleep(time.Second * 2)
	for i := 0; i < defaultInitialLimiterCapacity1; i++ {
		flag := flowLimiter.Allow()
		re.Equal(true, flag)
	}

	flag = flowLimiter.Allow()
	re.Equal(false, flag)

	time.Sleep(time.Second)
	for i := 0; i < defaultInitialLimiterRate1; i++ {
		flag := flowLimiter.Allow()
		re.Equal(true, flag)
	}

	flag = flowLimiter.Allow()
	re.Equal(false, flag)
}
