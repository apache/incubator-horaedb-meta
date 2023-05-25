// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package limiter

import (
	"sync"

	"github.com/CeresDB/ceresmeta/server/config"
	"golang.org/x/time/rate"
)

type FlowLimiter struct {
	l *rate.Limiter
	// RWMutex is used to protect following fields.
	lock                          sync.RWMutex
	tokenBucketFillRate           int
	tokenBucketBurstEventCapacity int
	enable                        bool
	unLimitList                   map[string]string
}

func NewFlowLimiter(config config.LimiterConfig) *FlowLimiter {
	newLimiter := rate.NewLimiter(rate.Limit(config.TokenBucketFillRate), config.TokenBucketBurstEventCapacity)
	unLimitList := make(map[string]string)
	for _, method := range config.UnLimitList {
		unLimitList[method] = method
	}
	return &FlowLimiter{
		l:                             newLimiter,
		tokenBucketFillRate:           config.TokenBucketFillRate,
		tokenBucketBurstEventCapacity: config.TokenBucketBurstEventCapacity,
		enable:                        config.Enable,
		unLimitList:                   unLimitList,
	}
}

func (f *FlowLimiter) Allow(method string) bool {
	if !f.enable {
		return true
	}
	if _, ok := f.unLimitList[method]; ok {
		return true
	}
	return f.l.Allow()
}

func (f *FlowLimiter) UpdateLimiter(config config.LimiterConfig) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.l.SetLimit(rate.Limit(config.TokenBucketFillRate))
	f.l.SetBurst(config.TokenBucketBurstEventCapacity)
	f.tokenBucketFillRate = config.TokenBucketFillRate
	f.tokenBucketBurstEventCapacity = config.TokenBucketBurstEventCapacity
	f.enable = config.Enable
	return nil
}

func (f *FlowLimiter) UpdateUnLimitList(unLimitMethods []string, limitMethods []string) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	for _, unLimitMethod := range unLimitMethods {
		f.unLimitList[unLimitMethod] = unLimitMethod
	}

	for _, limitMethod := range limitMethods {
		delete(f.unLimitList, limitMethod)
	}

	return nil
}
