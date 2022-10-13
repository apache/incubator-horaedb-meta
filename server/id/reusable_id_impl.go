// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

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
	existIDs *OrderedList
}

type OrderedList struct {
	sortedArray []uint64
}

func (l *OrderedList) IndexOfValue(v uint64) int {
	for i, value := range l.sortedArray {
		if value == v {
			return i
		}
	}
	return -1
}

func (l *OrderedList) Insert(v uint64) {
	l.sortedArray = append(l.sortedArray, v)
	sort.Slice(l.sortedArray, func(i, j int) bool {
		return l.sortedArray[i] < l.sortedArray[j]
	})
}

func (l *OrderedList) Get(i int) uint64 {
	return l.sortedArray[i]
}

func (l *OrderedList) Remove(v uint64) int {
	removeIndex := -1
	for i, value := range l.sortedArray {
		if value == v {
			removeIndex = i
		}
	}
	l.sortedArray = append(l.sortedArray[:removeIndex], l.sortedArray[removeIndex+1:]...)
	return removeIndex
}

func (l *OrderedList) Length() int {
	return len(l.sortedArray)
}

func (l *OrderedList) Min() uint64 {
	return l.sortedArray[0]
}

func NewReusableAllocatorImpl(existIDs []uint64, minID uint64) Allocator {
	return &ReusableAllocatorImpl{minID: minID, existIDs: &OrderedList{sortedArray: existIDs}}
}

func (a *ReusableAllocatorImpl) Alloc(_ context.Context) (uint64, error) {
	a.lock.Lock()
	defer a.lock.Unlock()
	var next uint64
	// find minimum unused ID bigger than minID
	if a.existIDs.Length() == 0 || a.existIDs.Min() > a.minID {
		next = a.minID
		a.existIDs.Insert(next)
		return next, nil
	}
	for i := 0; i < a.existIDs.Length(); i++ {
		if i == a.existIDs.Length()-1 || a.existIDs.Get(i)+1 != a.existIDs.Get(i+1) {
			next = a.existIDs.Get(i) + 1
			break
		}
	}
	a.existIDs.Insert(next)
	return next, nil
}

func (a *ReusableAllocatorImpl) Collect(_ context.Context, id uint64) error {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.existIDs.Remove(id)
	return nil
}
