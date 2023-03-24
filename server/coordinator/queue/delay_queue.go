// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package queue

import (
	"fmt"
	"sync"
	"time"

	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/pkg/errors"
)

type ProcedureWithTime struct {
	procedure procedure.Procedure
	time      time.Time
}

type ProcedureDelayQueue struct {
	lock       sync.RWMutex
	procedures []*ProcedureWithTime
	maxLen     int
}

func NewProcedureDelayQueue(maxLen int) *ProcedureDelayQueue {
	return &ProcedureDelayQueue{
		procedures: []*ProcedureWithTime{},
		maxLen:     maxLen,
	}
}

func (q *ProcedureDelayQueue) Len() int {
	q.lock.RLock()
	defer q.lock.RUnlock()

	return len(q.procedures)
}

func (q *ProcedureDelayQueue) Offer(p procedure.Procedure, delaySec time.Duration) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	if len(q.procedures) >= q.maxLen {
		return errors.WithMessage(ErrQueueFull, fmt.Sprintf("queue max length is %d", q.maxLen))
	}

	for _, procedureWithTime := range q.procedures {
		if procedureWithTime.procedure.ID() == p.ID() {
			return errors.WithMessage(ErrOfferDuplicatedProcedure, fmt.Sprintf("procedure has been offered, %v", p))
		}
	}

	q.procedures = append(q.procedures, &ProcedureWithTime{
		procedure: p,
		time:      time.Now().Add(delaySec),
	})

	return nil
}

func (q *ProcedureDelayQueue) Poll() procedure.Procedure {
	q.lock.Lock()
	defer q.lock.Unlock()

	for i, procedureWithTime := range q.procedures {
		if procedureWithTime.time.Unix() <= time.Now().Unix() {
			procedure := procedureWithTime.procedure
			q.procedures = append(q.procedures[:i], q.procedures[i+1:]...)
			return procedure
		}
	}
	return nil
}
