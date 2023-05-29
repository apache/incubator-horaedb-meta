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
)

func TestFlowLimiter(t *testing.T) {
	re := require.New(t)
	flowLimiter := NewFlowLimiter(config.LimiterConfig{
		TokenBucketFillRate:           defaultInitialLimiterRate,
		TokenBucketBurstEventCapacity: defaultInitialLimiterCapacity,
		Enable:                        defaultEnableLimiter,
	})

	for i := 0; i < defaultInitialLimiterCapacity; i++ {
		flag := flowLimiter.Allow()
		re.Equal(true, flag)
	}

	flag := flowLimiter.Allow()
	re.Equal(false, flag)

	time.Sleep(time.Second)
	for i := 0; i < defaultInitialLimiterRate; i++ {
		flag := flowLimiter.Allow()
		re.Equal(true, flag)
	}

	flag = flowLimiter.Allow()
	re.Equal(false, flag)

	err := flowLimiter.UpdateLimiter(config.LimiterConfig{
		TokenBucketFillRate:           defaultUpdateLimiterRate,
		TokenBucketBurstEventCapacity: defaultUpdateLimiterCapacity,
		Enable:                        defaultEnableLimiter,
	})
	re.NoError(err)

	time.Sleep(time.Second)
	for i := 0; i < defaultUpdateLimiterRate; i++ {
		flag := flowLimiter.Allow()
		re.Equal(true, flag)
	}

	flag = flowLimiter.Allow()
	re.Equal(false, flag)
}
