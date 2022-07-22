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
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const defaultAllocStep = uint64(1000)

type AllocatorImpl struct {
	// lock var base&end when allocator ID
	sync.Mutex
	base     uint64
	end      uint64
	kv       storage.KV
	rootPath string
	key      string
}

func NewAllocatorImpl(kv storage.KV, rootPath string, key string) *AllocatorImpl {
	return &AllocatorImpl{kv: kv, key: key, rootPath: rootPath}
}

func (alloc *AllocatorImpl) Alloc(ctx context.Context) (uint64, error) {
	alloc.Lock()
	defer alloc.Unlock()

	if alloc.base == alloc.end {
		if err := alloc.fastRebaseLocked(ctx); err != nil {
			if err := alloc.rebaseLocked(ctx); err != nil {
				return 0, err
			}
		}
	}

	alloc.base++
	return alloc.base, nil
}

func (alloc *AllocatorImpl) rebaseLocked(ctx context.Context) error {
	value, err := alloc.kv.Get(ctx, alloc.key)
	if err != nil {
		return errors.Wrapf(err, "get base id failed, key:%v", alloc.key)
	}
	if value != "" {
		value, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return errors.Wrapf(err, "convert string to int failed")
		}
		return alloc.idAllocBase(ctx, value)
	}

	return alloc.idAllocBase(ctx, 0)
}

func (alloc *AllocatorImpl) fastRebaseLocked(ctx context.Context) error {
	return alloc.idAllocBase(ctx, alloc.end)
}

func (alloc *AllocatorImpl) idAllocBase(ctx context.Context, value uint64) error {
	end := value + defaultAllocStep
	key := path.Join(alloc.rootPath, alloc.key)

	var cmp clientv3.Cmp
	if value == 0 {
		cmp = clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	} else {
		cmp = clientv3.Compare(clientv3.Value(key), "=", fmt.Sprintf("%020d", value))
	}
	opPutBaseID := clientv3.OpPut(key, fmt.Sprintf("%020d", end))

	resp, err := alloc.kv.Txn(ctx).
		If(cmp).
		Then(opPutBaseID).
		Commit()
	if err != nil {
		return errors.Wrapf(err, "put base id failed, key:%v", key)
	} else if !resp.Succeeded {
		return ErrTxnPutBaseID.WithCausef("txn put base id failed, resp:%v", resp)
	}

	log.Info("Allocator allocates a new id", zap.Uint64("alloc-id", end))

	alloc.end = end
	alloc.base = end - defaultAllocStep

	return nil
}
