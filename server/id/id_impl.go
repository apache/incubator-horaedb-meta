// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package id

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/storage"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	defaultallocStep = uint64(1000)
	defaultkey       = "alloc_id"
)

type AllocatorImpl struct {
	mu   sync.Mutex
	base uint64
	end  uint64
	kv   storage.KV
}

func NewAllocatorImpl(client *clientv3.Client, rootPath string) *AllocatorImpl {
	return &AllocatorImpl{kv: storage.NewEtcdKV(client, rootPath)}
}

func (alloc *AllocatorImpl) Alloc(ctx context.Context) (uint64, error) {
	alloc.mu.Lock()
	defer alloc.mu.Unlock()
	if alloc.base == alloc.end {
		if err := alloc.rebaseLocked(ctx); err != nil {
			return 0, err
		}
	}
	alloc.base++
	return alloc.base, nil
}

func (alloc *AllocatorImpl) Rebase(ctx context.Context) error {
	alloc.mu.Lock()
	defer alloc.mu.Unlock()
	return alloc.rebaseLocked(ctx)
}

func (alloc *AllocatorImpl) rebaseLocked(ctx context.Context) error {
	value, err := alloc.kv.Get(ctx, defaultkey)
	if err != nil {
		return err
	}
	var end uint64
	if value == "" {
		end = 0
	} else {
		value, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
		end = uint64(value)
	}
	end += defaultallocStep
	err = alloc.kv.Put(ctx, defaultkey, fmt.Sprintf("%020d", end))
	if err != nil {
		return err
	}
	log.Info("Allocator allocates a new id", zap.Uint64("alloc-id", end))
	alloc.end = end
	alloc.base = end - defaultallocStep
	return nil
}
