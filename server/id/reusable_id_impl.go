package id

import (
	"context"
	"sort"
	"sync"
)

type ReusableAllocatorImpl struct {
	// RWMutex is used to protect following fields.
	lock sync.Mutex

	minID    uint64
	existIDs []uint64
}

func NewReusableAllocatorImpl(existIDs []uint64, minID uint64) *ReusableAllocatorImpl {
	return &ReusableAllocatorImpl{minID: minID, existIDs: existIDs}
}

func (a *ReusableAllocatorImpl) Alloc(ctx context.Context) (uint64, error) {
	a.lock.Lock()
	defer a.lock.Unlock()
	var next uint64
	// sort existIDs, find minimum number bigger than minID
	sort.Slice(a.existIDs, func(i, j int) bool {
		return a.existIDs[i] < a.existIDs[j]
	})
	if a.existIDs[0] > a.minID {
		next = a.minID
		a.existIDs = append(a.existIDs, next)
		return next, nil
	}
	for i := 0; i < len(a.existIDs); i++ {
		if i == len(a.existIDs)-1 || a.existIDs[i]+1 != a.existIDs[i+1] {
			next = a.existIDs[i] + 1
			break
		}
	}
	a.existIDs = append(a.existIDs, next)
	return next, nil
}

func (a *ReusableAllocatorImpl) Collect(ctx context.Context, id uint64) error {
	a.lock.Lock()
	defer a.lock.Unlock()
	for i := 0; i < len(a.existIDs); i++ {
		if a.existIDs[i] == id {
			a.existIDs = append(a.existIDs[:i], a.existIDs[i+1:]...)
		}
	}
	return nil
}
