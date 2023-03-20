package procedure

import "time"

type WaitingProcedureQueue interface {
	Add(procedure Procedure) error

	Deque() Procedure
}

// WaitingProcedureQueueImpl is used to manager procedures on specify shard.
type WaitingProcedureQueueImpl struct {
	shardID    uint64
	procedures []Procedure

	maxLength   uint64
	expiredTime time.Duration
}

func (q WaitingProcedureQueueImpl) expired() {

}
