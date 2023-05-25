// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package limiter

import (
	"testing"
	"time"

	"github.com/CeresDB/ceresmeta/server/config"
	"github.com/stretchr/testify/require"
)

const (
	defaultInitialLimiterRate     = 10
	defaultInitialLimiterCapacity = 1000
	defaultEnableLimiter          = true
	defaultUpdateLimiterRate      = 100
	defaultUpdateLimiterCapacity  = 100
	defaultUnLimitMethod          = "aaa"
	defaultLimitMethod            = "bbb"
)

func TestFlowLimiter(t *testing.T) {
	re := require.New(t)
	flowLimiter := NewFlowLimiter(config.LimiterConfig{
		TokenBucketFillRate:           defaultInitialLimiterRate,
		TokenBucketBurstEventCapacity: defaultInitialLimiterCapacity,
		Enable:                        defaultEnableLimiter,
		UnLimitList:                   make([]string, 0),
	})

	for i := 0; i < defaultInitialLimiterCapacity; i++ {
		flag := flowLimiter.Allow(defaultLimitMethod)
		re.Equal(true, flag)
	}

	flag := flowLimiter.Allow(defaultLimitMethod)
	re.Equal(false, flag)

	time.Sleep(time.Second)
	for i := 0; i < defaultInitialLimiterRate; i++ {
		flag := flowLimiter.Allow(defaultLimitMethod)
		re.Equal(true, flag)
	}

	flag = flowLimiter.Allow(defaultLimitMethod)
	re.Equal(false, flag)

	unLimitMethods := make([]string, 1)
	unLimitMethods = append(unLimitMethods, defaultUnLimitMethod)
	limitMethods := make([]string, 1)
	limitMethods = append(limitMethods, defaultLimitMethod)

	err := flowLimiter.UpdateUnLimitList(unLimitMethods, limitMethods)
	re.NoError(err)

	err = flowLimiter.UpdateLimiter(config.LimiterConfig{
		TokenBucketFillRate:           defaultUpdateLimiterRate,
		TokenBucketBurstEventCapacity: defaultUpdateLimiterCapacity,
		Enable:                        defaultEnableLimiter,
	})
	re.NoError(err)

	time.Sleep(time.Second)
	for i := 0; i < defaultUpdateLimiterRate; i++ {
		flag := flowLimiter.Allow(defaultLimitMethod)
		re.Equal(true, flag)
	}

	flag = flowLimiter.Allow(defaultLimitMethod)
	re.Equal(false, flag)

	for i := 0; i < defaultUpdateLimiterCapacity*2; i++ {
		flag := flowLimiter.Allow(defaultUnLimitMethod)
		re.Equal(true, flag)
	}
}
