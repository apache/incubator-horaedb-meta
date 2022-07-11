// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package id

import (
	"context"
	"path"
	"sync"

	"github.com/CeresDB/ceresmeta/pkg/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const allocStep = uint64(1000)

type AllocatorImpl struct {
	mu   sync.Mutex
	base uint64
	end  uint64

	client   *clientv3.Client
	rootPath string
	member   string
}

func NewAllocatorImpl(client *clientv3.Client, rootPath string, member string) *AllocatorImpl {
	return &AllocatorImpl{client: client, rootPath: rootPath, member: member}
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
	value := resp.Kvs[0].Value
	var (
		cmp clientv3.Cmp
		end uint64
	)

	if value == nil {
		// create the key
		cmp = clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	} else {
		// update the key
		end, err = uint64(value[])
		if err != nil {
			return err
		}

		cmp = clientv3.Compare(clientv3.Value(key), "=", string(value))
	}

	end += allocStep
	value = typeutil.Uint64ToBytes(end)
	txn := kv.NewSlowLogTxn(alloc.client)
	leaderPath := path.Join(alloc.rootPath, "leader")
	t := txn.If(append([]clientv3.Cmp{cmp}, clientv3.Compare(clientv3.Value(leaderPath), "=", alloc.member))...)
	resp, err := t.Then(clientv3.OpPut(key, string(value))).Commit()
	if err != nil {
		return errs.ErrEtcdTxnInternal.Wrap(err).GenWithStackByArgs()
	}
	if !resp.Succeeded {
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}

	log.Info("idAllocator allocates a new id", zap.Uint64("alloc-id", end))
	idallocGauge.Set(float64(end))
	alloc.end = end
	alloc.base = end - allocStep
	return nil
}

func (alloc *AllocatorImpl) getAllocIDPath() string {
	return path.Join(alloc.rootPath, "alloc_id")
}
