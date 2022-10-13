// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package id

import (
	"context"
	"sort"
	"sync"
)

type ReusableAllocatorImpl struct {
	// Mutex is used to protect following fields.
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

func (l *OrderedList) FindFirstHoleValueAndIndex(min uint64) (uint64, int) {
	if len(l.sortedArray) == 0 {
		return min, 0
	}
	if len(l.sortedArray) == 1 {
		return l.sortedArray[0] + 1, 1
	}
	if l.sortedArray[0] > min {
		return min, 0
	}

	s := l.sortedArray
	for i := 0; i < len(l.sortedArray)-1; i++ {
		if s[i]+1 != s[i+1] {
			return s[i] + 1, i + 1
		}
	}

	return s[len(s)-1] + 1, len(s)
}

func (l *OrderedList) Insert(v uint64, i int) {
	if len(l.sortedArray) == i {
		l.sortedArray = append(l.sortedArray, v)
	} else {
		l.sortedArray = append(l.sortedArray[:i+1], l.sortedArray[i:]...)
		l.sortedArray[i] = v
	}
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
	sort.Slice(existIDs, func(i, j int) bool {
		return existIDs[i] < existIDs[j]
	})
	return &ReusableAllocatorImpl{minID: minID, existIDs: &OrderedList{sortedArray: existIDs}}
}

func (a *ReusableAllocatorImpl) Alloc(_ context.Context) (uint64, error) {
	a.lock.Lock()
	defer a.lock.Unlock()
	// Find minimum unused ID bigger than minID
	v, i := a.existIDs.FindFirstHoleValueAndIndex(a.minID)
	a.existIDs.Insert(v, i)
	return v, nil
}

func (a *ReusableAllocatorImpl) Collect(_ context.Context, id uint64) error {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.existIDs.Remove(id)
	return nil
}
