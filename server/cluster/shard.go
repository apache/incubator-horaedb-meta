// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"sync"
	"time"
)

type Shard struct {
	meta    []*clusterpb.Shard
	nodes   []*clusterpb.Node
	tables  map[uint64]*Table // table_id => table
	version uint64
}

func (s *Shard) dropTableLocked(tableID uint64) {
	delete(s.tables, tableID)
}

type ShardTablesWithRole struct {
	shardID   uint32
	shardRole clusterpb.ShardRole
	tables    []*Table
	version   uint64
}

type ShardWithLock struct {
	shardID uint32
	lock    sync.Mutex
}

var (
	shardLockMap = make(map[uint32]*ShardWithLock)
	mapLock      = sync.Mutex{}
)

func getShardLock(shardID uint32) *ShardWithLock {
	mapLock.Lock()
	defer mapLock.Unlock()
	lock, ok := shardLockMap[shardID]
	if ok {
		return lock
	} else {
		shardLockMap[shardID] = &ShardWithLock{shardID: shardID, lock: sync.Mutex{}}
	}
	return shardLockMap[shardID]
}

func LockShardByID(shardID uint32) bool {
	shardLock := getShardLock(shardID)
	return shardLock.lock.TryLock()
}

func LockShardByIDWithRetry(shardID uint32, maxRetrySize int, waitDuration time.Duration) bool {
	lockResult := LockShardByID(shardID)
	// if lock failed, wait 1 seconds and retry
	if maxRetrySize == 0 {
		return false
	}
	if !lockResult {
		time.Sleep(waitDuration)
		return LockShardByIDWithRetry(shardID, maxRetrySize-1, waitDuration)
	} else {
		return true
	}
}

func UnlockShardByID(shardID uint32) {
	shardLock := getShardLock(shardID)
	if shardLock == nil {
		return
	}
	shardLock.lock.Unlock()
	delete(shardLockMap, shardID)
}
