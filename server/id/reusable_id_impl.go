package id

import (
	"context"
	"sync"
)

type ReusableAllocatorImpl struct {
	// RWMutex is used to protect following fields.
	lock sync.Mutex

	idQueue     []uint64
	maxShardNum uint64
}

func NewReusableAllocatorImpl(maxShardNum uint64) *ReusableAllocatorImpl {
	// Init id queue
	var queue []uint64
	for i := uint64(0); i < maxShardNum; i++ {
		queue = append(queue, i)
	}

	return &ReusableAllocatorImpl{
		idQueue:     queue,
		maxShardNum: maxShardNum,
	}
}

func (a *ReusableAllocatorImpl) isExhausted() bool {
	return len(a.idQueue) == 0
}

func (a *ReusableAllocatorImpl) Collect(id uint64) error {
	a.lock.Lock()
	defer a.lock.Unlock()
	if id >= a.maxShardNum {
		return ErrCollectID.WithCausef("ID is invalid, id:%d can not greater than maxShardNum:%d", id, a.maxShardNum)
	}
	a.idQueue = append(a.idQueue, id)
	return nil
}

func (a *ReusableAllocatorImpl) Alloc(ctx context.Context) (uint64, error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	if a.isExhausted() {
		return 0, ErrAllocID.WithCausef("ID is exhausted, maxShardNum:%d", a.maxShardNum)
	}

	ret := a.idQueue[0]
	a.idQueue = a.idQueue[1:]

	return ret, nil
}
