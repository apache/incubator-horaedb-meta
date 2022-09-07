// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/looplab/fsm"
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

// Shard FSM Const Definition
// It contains the event name and the state name
const (
	EventTransferLeader           = "TransferLeader"
	EventTransferToFollowerFailed = "TransferToFollowerFailed"
	EventTransferFollower         = "TransferFollower"
	EventTransferToLeaderFailed   = "TransferToLeaderFailed"
	EventTransferFollowerStart    = "TransferFollowerStart"
	EventTransferLeaderStart      = "TransferLeaderStart"
	StateLeader                   = "LEADER"
	StateFollower                 = "FOLLOWER"
	StatePendingLeader            = "PENDING_LEADER"
	StatePendingFollower          = "PENDING_FOLLOWER"
)

// NewFSM /**
/**
```
┌────┐                   ┌────┐
│ RW ├─────────┐         │ R  ├─────────┐
├────┘         │         ├────┘         │
│    Leader    ◀─────────│PendingLeader │
│              │         │              │
└───────┬──▲───┘         └───────▲─┬────┘
        │  │                     │ │
┌────┐  │  │             ┌────┐  │ │
│ R  ├──▼──┴───┐         │ R  ├──┴─▼────┐
├────┘         │         ├────┘         │
│   Pending    ├─────────▶   Follower   │
│   Follower   │         │              │
└──────────────┘         └──────────────┘
```
*/
func NewFSM(role clusterpb.ShardRole) *fsm.FSM {
	shardFsm := fsm.NewFSM(
		StateFollower,
		fsm.Events{
			{Name: EventTransferLeader, Src: []string{StatePendingLeader}, Dst: StateLeader},
			{Name: EventTransferToFollowerFailed, Src: []string{StatePendingFollower}, Dst: StateLeader},
			{Name: EventTransferFollower, Src: []string{StatePendingFollower}, Dst: StateFollower},
			{Name: EventTransferToLeaderFailed, Src: []string{StatePendingLeader}, Dst: StateFollower},
			{Name: EventTransferFollowerStart, Src: []string{StateLeader}, Dst: StatePendingFollower},
			{Name: EventTransferLeaderStart, Src: []string{StateFollower}, Dst: StatePendingLeader},
		},
		fsm.Callbacks{},
	)
	if role == clusterpb.ShardRole_LEADER {
		shardFsm.SetState(StateLeader)
	}
	if role == clusterpb.ShardRole_FOLLOWER {
		shardFsm.SetState(StateFollower)
	}
	return shardFsm
}
