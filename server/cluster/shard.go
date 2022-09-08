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
	EventTransferLeader          = "TransferLeader"
	EventTransferFollowerFailed  = "TransferFollowerFailed"
	EventTransferFollower        = "TransferFollower"
	EventTransferLeaderFailed    = "TransferLeaderFailed"
	EventPrepareTransferFollower = "PrepareTransferFollower"
	EventPrepareTransferLeader   = "PrepareTransferLeader"
	StateLeader                  = "LEADER"
	StateFollower                = "FOLLOWER"
	StatePendingLeader           = "PENDING_LEADER"
	StatePendingFollower         = "PENDING_FOLLOWER"
)

// Declare the source state array of FSM, avoid creating arrays repeatedly every time you create an FSM
var (
	EventTransferLeaderSrc          = []string{StatePendingLeader}
	EventTransferFollowerFailedSrc  = []string{StatePendingFollower}
	EventTransferFollowerSrc        = []string{StatePendingFollower}
	EventTransferLeaderFailedSrc    = []string{StatePendingLeader}
	EventPrepareTransferFollowerSrc = []string{StateLeader}
	EventPrepareTransferLeaderSrc   = []string{StateFollower}
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
			{Name: EventTransferLeader, Src: EventTransferLeaderSrc, Dst: StateLeader},
			{Name: EventTransferFollowerFailed, Src: EventTransferFollowerFailedSrc, Dst: StateLeader},
			{Name: EventTransferFollower, Src: EventTransferFollowerSrc, Dst: StateFollower},
			{Name: EventTransferLeaderFailed, Src: EventTransferLeaderFailedSrc, Dst: StateFollower},
			{Name: EventPrepareTransferFollower, Src: EventPrepareTransferFollowerSrc, Dst: StatePendingFollower},
			{Name: EventPrepareTransferLeader, Src: EventPrepareTransferLeaderSrc, Dst: StatePendingLeader},
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
