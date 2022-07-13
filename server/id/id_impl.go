// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package id

import (
	"context"
	"encoding/binary"
	"path"
	"sync"

	"github.com/CeresDB/ceresmeta/pkg/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const allocStep = uint64(1000)

type AllocatorImpl struct {
	mu       sync.Mutex
	base     uint64
	end      uint64
	client   *clientv3.Client
	rootPath string
}

func NewAllocatorImpl(client *clientv3.Client, rootPath string) *AllocatorImpl {
	return &AllocatorImpl{client: client, rootPath: rootPath}
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
	key := alloc.getAllocIDPath()
	resp, err := alloc.client.Get(ctx, key)
	if err != nil {
		return err
	}
	var end uint64
	if len(resp.Kvs) == 0 {
		end = 0
	} else {
		end = binary.BigEndian.Uint64(resp.Kvs[0].Value)
	}
	end += allocStep
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, end)
	_, err = alloc.client.Put(ctx, key, string(value))
	if err != nil {
		return err
	}
	log.Info("Allocator allocates a new id", zap.Uint64("alloc-id", end))
	alloc.end = end
	alloc.base = end - allocStep
	return nil
}

func (alloc *AllocatorImpl) getAllocIDPath() string {
	return path.Join(alloc.rootPath, "alloc_id")
}
