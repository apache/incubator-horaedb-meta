// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package service

import (
	"sync"

	"github.com/CeresDB/ceresmeta/server/config"
	"golang.org/x/time/rate"
)

type FlowLimiter struct {
	lock  sync.RWMutex
	l     *rate.Limiter
	rate  int
	burst int
}

func NewFlowLimiter(config config.LimiterConfig) *FlowLimiter {
	return &FlowLimiter{
		l:     rate.NewLimiter(rate.Limit(config.Rate), config.Burst),
		rate:  config.Rate,
		burst: config.Burst,
	}
}

func (f *FlowLimiter) GetThreshold() int {
	f.lock.RLocker()
	defer f.lock.RUnlock()

	return f.burst
}

func (f *FlowLimiter) Allow() bool {
	f.lock.RLocker()
	defer f.lock.RUnlock()

	return f.l.Allow()
}

func (f *FlowLimiter) UpdateLimiter(config config.LimiterConfig) {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.l.SetLimit(rate.Limit(config.Rate))
	f.l.SetBurst(config.Burst)
	f.rate = config.Rate
	f.burst = config.Burst
}
