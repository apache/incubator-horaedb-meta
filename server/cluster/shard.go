// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"context"
	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/server/schedule"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
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
	events = fsm.Events{
		{Name: EventTransferLeader, Src: []string{StatePendingLeader}, Dst: StateLeader},
		{Name: EventTransferFollowerFailed, Src: []string{StatePendingFollower}, Dst: StateLeader},
		{Name: EventTransferFollower, Src: []string{StatePendingFollower}, Dst: StateFollower},
		{Name: EventTransferLeaderFailed, Src: []string{StatePendingLeader}, Dst: StateFollower},
		{Name: EventPrepareTransferFollower, Src: []string{StateLeader}, Dst: StatePendingFollower},
		{Name: EventPrepareTransferLeader, Src: []string{StateFollower}, Dst: StatePendingLeader},
	}

	callbacks = fsm.Callbacks{
		EventTransferLeader: func(event *fsm.Event) {
			ctx := event.Args[0].(context.Context)
			c := event.Args[1].(*coordinator)
			newLeaderNode := event.Args[2].(string)
			oldLeaderShardId := event.Args[3].(uint32)
			newLeaderShardId := event.Args[4].(uint32)

			// Send event to CeresDB, waiting for response
			if err := c.eventHandler.Dispatch(ctx, newLeaderNode, &schedule.TransferLeaderEvent{OldLeaderShardID: oldLeaderShardId, NewLeaderShardID: newLeaderShardId}); err != nil {
				event.Cancel(errors.Wrap(err, EventTransferLeader))
			}
		},
		EventTransferFollowerFailed: func(event *fsm.Event) {
			ctx := event.Args[0].(context.Context)
			c := event.Args[1].(*coordinator)
			oldLeaderNode := event.Args[2].(string)
			oldLeaderShardId := event.Args[3].(uint32)
			newLeaderShardId := event.Args[4].(uint32)
			// Transfer failed, stop transfer and reset state
			if err := c.eventHandler.Dispatch(ctx, oldLeaderNode, &schedule.TransferLeaderFailedEvent{OldLeaderShardID: oldLeaderShardId, NewLeaderShardID: newLeaderShardId}); err != nil {
				event.Cancel(errors.Wrap(err, EventTransferFollowerFailed))
			}
		},
		EventTransferFollower: func(event *fsm.Event) {
			ctx := event.Args[0].(context.Context)
			c := event.Args[1].(*coordinator)
			oldLeaderNode := event.Args[2].(string)
			oldLeaderShardId := event.Args[3].(uint32)
			newLeaderShardId := event.Args[4].(uint32)

			if err := c.eventHandler.Dispatch(ctx, oldLeaderNode, &schedule.TransferLeaderEvent{OldLeaderShardID: oldLeaderShardId, NewLeaderShardID: newLeaderShardId}); err != nil {
				event.Cancel(errors.Wrap(err, EventTransferFollower))
			}
		},
		EventTransferLeaderFailed: func(event *fsm.Event) {
			// Transfer failed, stop transfer and reset state
			ctx := event.Args[0].(context.Context)
			c := event.Args[1].(*coordinator)
			newLeaderNode := event.Args[2].(string)
			oldLeaderShardId := event.Args[3].(uint32)
			newLeaderShardId := event.Args[4].(uint32)
			// Transfer failed, stop transfer and reset state
			if err := c.eventHandler.Dispatch(ctx, newLeaderNode, &schedule.TransferLeaderFailedEvent{OldLeaderShardID: oldLeaderShardId, NewLeaderShardID: newLeaderShardId}); err != nil {
				event.Cancel(errors.Wrap(err, EventTransferLeaderFailed))
			}
		},
		EventPrepareTransferFollower: func(event *fsm.Event) {
			ctx := event.Args[0].(context.Context)
			c := event.Args[1].(*coordinator)
			oldLeaderShardId := event.Args[2].(uint32)
			// Update Etcd
			if clusterTopology, err := c.cluster.storage.GetClusterTopology(ctx, c.cluster.clusterID); err != nil {
				event.Cancel(errors.Wrap(err, EventPrepareTransferFollower))
			} else {
				shardViews := clusterTopology.ShardView
				for _, shard := range shardViews {
					if shard.GetId() == oldLeaderShardId {
						//shard.ShardRole = clusterpb.ShardRole_PENDING_FOLLOWER
					}
				}
				c.cluster.storage.PutClusterTopology(ctx, c.cluster.clusterID, c.cluster.metaData.clusterTopology.Version, clusterTopology)
			}
		},
		EventPrepareTransferLeader: func(event *fsm.Event) {
			ctx := event.Args[0].(context.Context)
			c := event.Args[1].(*coordinator)
			newLeaderShardId := event.Args[2].(uint32)
			// Update Etcd
			if clusterTopology, err := c.cluster.storage.GetClusterTopology(ctx, c.cluster.clusterID); err != nil {
				event.Cancel(errors.Wrap(err, EventPrepareTransferLeader))
			} else {
				shardViews := clusterTopology.ShardView
				for _, shard := range shardViews {
					if shard.GetId() == newLeaderShardId {
						//shard.ShardRole = clusterpb.ShardRole_PENDING_LEADER
					}
				}
				c.cluster.storage.PutClusterTopology(ctx, c.cluster.clusterID, c.cluster.metaData.clusterTopology.Version, clusterTopology)
			}
		},
	}
)

// NewFSM
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
		events,
		callbacks,
	)
	if role == clusterpb.ShardRole_LEADER {
		shardFsm.SetState(StateLeader)
	}
	if role == clusterpb.ShardRole_FOLLOWER {
		shardFsm.SetState(StateFollower)
	}
	return shardFsm
}
