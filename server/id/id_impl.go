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

// When a process performs ID allocation,
// the lock will lock the shared resource area,
// so that other processes cannot temporarily perform ID allocation
var lock sync.Mutex

type AllocatorImpl struct {
	base     uint64
	end      uint64
	kv       storage.KV
	rootPath string
	key      string
}

// TODO: existed bug when rootPath = "/"
func NewAllocatorImpl(kv storage.KV, rootPath string, key string) *AllocatorImpl {
	return &AllocatorImpl{kv: kv, rootPath: rootPath, key: key}
}

func (alloc *AllocatorImpl) Alloc(ctx context.Context) (uint64, error) {
	lock.Lock()
	defer lock.Unlock()

	if alloc.base == alloc.end {
		if err := alloc.fastRebaseLocked(ctx); err != nil {
			log.Debug("fast rebase failed", zap.Error(err))

			if err := alloc.rebaseLocked(ctx); err != nil {
				return 0, err
			}
		}
	}

	ret := alloc.base
	alloc.base++
	return ret, nil
}

func (alloc *AllocatorImpl) rebaseLocked(ctx context.Context) error {
	currEnd, err := alloc.kv.Get(ctx, alloc.key)
	if err != nil {
		return errors.Wrapf(err, "get end id failed, key:%v", alloc.key)
	}

	if currEnd == "" {
		return ErrGetEndID.WithCausef("fail to get current end id, key not exist, key:%s", alloc.key)
	}

	return alloc.doRebase(ctx, decodeID(currEnd))
}

func (alloc *AllocatorImpl) fastRebaseLocked(ctx context.Context) error {
	return alloc.doRebase(ctx, alloc.end)
}

func (alloc *AllocatorImpl) doRebase(ctx context.Context, currEnd uint64) error {
	newEnd := currEnd + defaultAllocStep
	key := path.Join(alloc.rootPath, alloc.key)

	var cmp clientv3.Cmp
	if currEnd == 0 {
		cmp = clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	} else {
		cmp = clientv3.Compare(clientv3.Value(key), "=", encodeID(currEnd))
	}
	opPutEndID := clientv3.OpPut(key, encodeID(newEnd))

	// Check whether the currEnd id is existed in etcd, if not, create end id
	// Check whether the currEnd id is equal to that in etcd. If it is equal，update end id
	// otherwise return error
	resp, err := alloc.kv.Txn(ctx).
		If(cmp).
		Then(opPutEndID).
		Commit()
	if err != nil {
		return errors.Wrapf(err, "put end id failed, key:%v", key)
	} else if !resp.Succeeded {
		return ErrTxnPutEndID.WithCausef("txn put end id failed, resp:%v", resp)
	}

	alloc.end = newEnd
	alloc.base = newEnd - defaultAllocStep

	log.Info("Allocator allocates a new base id", zap.String("id-type", alloc.key), zap.Uint64("alloc-id", alloc.base))

	return nil
}

func encodeID(value uint64) string {
	return fmt.Sprintf("%d", value)
}

func decodeID(value string) uint64 {
	res, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		log.Error("convert string to int failed", zap.Error(err))
	}
	return res
}
