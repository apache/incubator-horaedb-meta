// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package id

import (
	"context"
	"fmt"
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
	//
	sync.Mutex
	base uint64
	end  uint64
	kv   storage.KV
	key  string
}

func NewAllocatorImpl(kv storage.KV, key string) *AllocatorImpl {
	return &AllocatorImpl{kv: kv, key: key}
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
	end := uint64(0)
	if value != "" {
		value, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return errors.Wrapf(err, "convert string to int failed")
		}
		end = uint64(value)
	}
	end += defaultAllocStep
	cmp := clientv3.Compare(clientv3.Value(func() string {
		value, err := alloc.kv.Get(ctx, alloc.key)
		if err != nil {
			log.Debug("get base id failed")
		}
		if value == "" {
			return "0"
		}
		return value
	}()), "=", fmt.Sprintf("%020d", end-defaultAllocStep))
	resp, err := alloc.kv.Txn(ctx).If(cmp).Then(clientv3.OpPut(alloc.key, fmt.Sprintf("%020d", end))).Commit()
	if err != nil {
		return errors.Wrapf(err, "put base id failed, key:%v", alloc.key)
	} else if !resp.Succeeded {
		return ErrTxnPutBaseId.WithCausef("txn put base id failed, resp:%v", resp)
	}
	log.Info("Allocator allocates a new id", zap.Uint64("alloc-id", end))
	alloc.end = end
	alloc.base = end - defaultAllocStep
	return nil
}

func (alloc *AllocatorImpl) fastRebaseLocked(ctx context.Context) error {
	end := alloc.end + defaultAllocStep
	cmp := clientv3.Compare(clientv3.Value(func() string {
		value, err := alloc.kv.Get(ctx, alloc.key)
		if err != nil {
			log.Debug("get base id failed")
		}
		if value == "" {
			return "0"
		}
		return value
	}()), "=", fmt.Sprintf("%020d", alloc.end))
	resp, err := alloc.kv.Txn(ctx).If(cmp).Then(clientv3.OpPut(alloc.key, fmt.Sprintf("%020d", end))).Commit()
	if err != nil {
		return errors.Wrapf(err, "put base id failed, key:%v", alloc.key)
	} else if !resp.Succeeded {
		return ErrTxnPutBaseId.WithCausef("txn put base id failed, resp:%v", resp)
	}
	log.Info("Allocator allocates a new id", zap.Uint64("alloc-id", end))
	alloc.end = end
	alloc.base = end - defaultAllocStep
	return nil
}
