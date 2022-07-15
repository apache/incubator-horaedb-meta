// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package id

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"sync"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const defaultallocStep = uint64(1000)

type AllocatorImpl struct {
	sync.Mutex
	base     uint64
	end      uint64
	kv       storage.KV
	rootPath string
}

func NewAllocatorImpl(kv storage.KV, rootPath string) *AllocatorImpl {
	return &AllocatorImpl{kv: kv, rootPath: rootPath}
}

func (alloc *AllocatorImpl) Alloc(ctx context.Context) (uint64, error) {
	alloc.Lock()
	defer alloc.Unlock()
	if alloc.base == alloc.end {
		if err := alloc.rebaseLocked(ctx); err != nil {
			return 0, err
		}
	}
	alloc.base++
	return alloc.base, nil
}

func (alloc *AllocatorImpl) Rebase(ctx context.Context) error {
	alloc.Lock()
	defer alloc.Unlock()
	return alloc.rebaseLocked(ctx)
}

func (alloc *AllocatorImpl) rebaseLocked(ctx context.Context) error {
	key := path.Join(alloc.rootPath, "alloc_id")
	value, err := alloc.kv.Get(ctx, key)
	if err != nil {
		return errors.Wrapf(err, "get base id err, key: %v", key)
	}
	var end uint64
	if value == "" {
		end = 0
	} else {
		value, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return errors.Wrapf(err, "convert string to int err")
		}
		end = uint64(value)
	}
	end += defaultallocStep
	err = alloc.kv.Put(ctx, key, fmt.Sprintf("%020d", end))
	if err != nil {
		return errors.Wrapf(err, "put base id err, key: %v", key)
	}
	log.Info("Allocator allocates a new id", zap.Uint64("alloc-id", end))
	alloc.end = end
	alloc.base = end - defaultallocStep
	return nil
}
