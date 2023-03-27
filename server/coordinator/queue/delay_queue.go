// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package queue

import (
	"container/heap"
	"fmt"
	"sync"
	"time"

	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/pkg/errors"
)

type ProcedureScheduleEntry struct {
	procedure procedure.Procedure
	runAfter  time.Time
}

type ProcedureDelayQueue struct {
	lock      sync.RWMutex
	heapQueue *HeapPriorityQueue
	index     map[uint64]procedure.Procedure
	maxLen    int
}

type HeapPriorityQueue struct {
	lock sync.RWMutex

	procedures []*ProcedureScheduleEntry
}

func (q *HeapPriorityQueue) Len() int {
	return len(q.procedures)
}

func (q *HeapPriorityQueue) Less(i, j int) bool {
	if q.procedures[i].runAfter.Before(q.procedures[j].runAfter) {
		return true
	}
	return false
}

func (q *HeapPriorityQueue) Swap(i, j int) {
	q.procedures[i], q.procedures[j] = q.procedures[j], q.procedures[i]
}

func (q *HeapPriorityQueue) Push(x any) {
	item := x.(*ProcedureScheduleEntry)
	q.procedures = append(q.procedures, item)
}

func (q *HeapPriorityQueue) Pop() any {
	length := len(q.procedures)
	if length == 0 {
		return nil
	}
	item := q.procedures[length-1]
	q.procedures = append(q.procedures[:length-1], q.procedures[length:]...)
	return item
}

func NewProcedureDelayQueue(maxLen int) *ProcedureDelayQueue {
	return &ProcedureDelayQueue{
		heapQueue: &HeapPriorityQueue{procedures: []*ProcedureScheduleEntry{}},
		maxLen:    maxLen,
		index:     map[uint64]procedure.Procedure{},
	}
}

func (q *ProcedureDelayQueue) Len() int {
	q.lock.RLock()
	defer q.lock.RUnlock()

	return q.heapQueue.Len()
}

func (q *ProcedureDelayQueue) Push(p procedure.Procedure, delay time.Duration) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.heapQueue.Len() >= q.maxLen {
		return errors.WithMessage(ErrQueueFull, fmt.Sprintf("queue max length is %d", q.maxLen))
	}

	for _, procedureWithTime := range q.heapQueue.procedures {
		if procedureWithTime.procedure.ID() == p.ID() {
			return errors.WithMessage(ErrPushDuplicatedProcedure, fmt.Sprintf("procedure has been pushed, %v", p))
		}
	}

	heap.Push(q.heapQueue, &ProcedureScheduleEntry{
		procedure: p,
		runAfter:  time.Now().Add(delay),
	})
	q.index[p.ID()] = p

	return nil
}

func (q *ProcedureDelayQueue) Pop() procedure.Procedure {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.heapQueue.Len() == 0 {
		return nil
	}

	entry := heap.Pop(q.heapQueue).(*ProcedureScheduleEntry)
	if time.Now().Before(entry.runAfter) {
		heap.Push(q.heapQueue, entry)
		return nil
	}

	delete(q.index, entry.procedure.ID())
	return entry.procedure
}
