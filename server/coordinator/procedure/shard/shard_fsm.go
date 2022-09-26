// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package shard

import (
	"context"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/dispatch"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
)

// Shard FSM Const Definition
// It contains the event name and the state name
const (
	StateLeader          = "LEADER"
	StateFollower        = "FOLLOWER"
	StatePendingLeader   = "PENDING_LEADER"
	StatePendingFollower = "PENDING_FOLLOWER"

	EventPrepareTransferFollower = "PrepareTransferFollower"
	EventTransferFollower        = "TransferFollower"
	EventTransferFollowerFailed  = "TransferFollowerFailed"

	EventPrepareTransferLeader = "PrepareTransferLeader"
	EventTransferLeader        = "TransferLeader"
	EventTransferLeaderFailed  = "TransferLeaderFailed"
)

// Declare the source state array of FSM, avoid creating arrays repeatedly every time you create an FSM
var (
	leaderFsmEvent = fsm.Events{
		{Name: EventPrepareTransferFollower, Src: []string{StateLeader}, Dst: StatePendingFollower},
		{Name: EventTransferFollower, Src: []string{StatePendingFollower}, Dst: StateFollower},
		{Name: EventTransferFollowerFailed, Src: []string{StatePendingFollower}, Dst: StateLeader},
	}
	leaderFsmCallbacks = fsm.Callbacks{
		EventPrepareTransferFollower: func(event *fsm.Event) {
			request := event.Args[0].(*LeaderCallbackRequest)
			ctx := request.Ctx
			c := request.C
			oldLeaderShardID := request.OldLeaderShardID

			// Update Etcd
			if clusterTopology, err := c.GetStorage().GetClusterTopology(ctx, c.GetClusterID()); err != nil {
				event.Cancel(errors.Wrap(err, EventPrepareTransferFollower))
			} else {
				shardViews := clusterTopology.ShardView
				for _, shard := range shardViews {
					// nolint
					if shard.GetId() == oldLeaderShardID {
						// TODO: add ShardRole enum in clusterpb
						// shard.ShardRole = clusterpb.ShardRole_PENDING_FOLLOWER
					}
				}
				if err := c.GetStorage().PutClusterTopology(ctx, c.GetClusterID(), c.GetClusterVersion(), clusterTopology); err != nil {
					event.Cancel(errors.Wrap(err, EventPrepareTransferFollower))
				}
			}
		},
		EventTransferFollower: func(event *fsm.Event) {
			request := event.Args[0].(*LeaderCallbackRequest)
			dispatch := request.Dispatch
			oldLeaderNode := request.OldLeaderNode
			oldLeaderShardID := request.OldLeaderShardID

			if result, err := dispatch.SendCloseEvent([]uint32{oldLeaderShardID}, oldLeaderNode); err != nil || !result {
				event.Cancel(errors.Wrap(err, EventTransferFollowerFailed))
			}
		},
		EventTransferFollowerFailed: func(event *fsm.Event) {
			request := event.Args[0].(*LeaderCallbackRequest)
			disPatch := request.Dispatch
			oldLeaderNode := request.OldLeaderNode
			oldLeaderShardID := request.OldLeaderShardID

			// Transfer failed, stop transfer and reset state
			if result, err := disPatch.SendOpenEvent([]uint32{oldLeaderShardID}, oldLeaderNode); err != nil || !result {
				event.Cancel(errors.Wrap(err, EventTransferFollowerFailed))
			}
		},
	}

	followerFsmEvent = fsm.Events{
		{Name: EventPrepareTransferLeader, Src: []string{StateFollower}, Dst: StatePendingLeader},
		{Name: EventTransferLeader, Src: []string{StatePendingLeader}, Dst: StateLeader},
		{Name: EventTransferLeaderFailed, Src: []string{StatePendingLeader}, Dst: StateFollower},
	}
	followerFsmCallbacks = fsm.Callbacks{
		EventPrepareTransferLeader: func(event *fsm.Event) {
			request := event.Args[0].(*FollowerCallbackRequest)
			ctx := request.Ctx
			c := request.C
			newLeaderShardID := request.NewLeaderShardID

			// Update Etcd
			if clusterTopology, err := c.GetStorage().GetClusterTopology(ctx, c.GetClusterID()); err != nil {
				event.Cancel(errors.Wrap(err, EventPrepareTransferLeader))
			} else {
				shardViews := clusterTopology.ShardView
				for _, shard := range shardViews {
					// nolint
					if shard.GetId() == newLeaderShardID {
						// TODO: add ShardRole enum in clusterpb
						// shard.ShardRole = clusterpb.ShardRole_PENDING_LEADER
					}
				}
				if err := c.GetStorage().PutClusterTopology(ctx, c.GetClusterID(), c.GetClusterVersion(), clusterTopology); err != nil {
					event.Cancel(errors.Wrap(err, EventPrepareTransferLeader))
				}
			}
		},
		EventTransferLeader: func(event *fsm.Event) {
			request := event.Args[0].(*FollowerCallbackRequest)
			dispatch := request.Dispatch
			newLeaderNode := request.NewLeaderNode
			newLeaderShardID := request.NewLeaderShardID

			// Send event to CeresDB, waiting for response
			if result, err := dispatch.SendOpenEvent([]uint32{newLeaderShardID}, newLeaderNode); err != nil || !result {
				event.Cancel(errors.Wrap(err, EventTransferLeader))
			}
		},
		EventTransferLeaderFailed: func(event *fsm.Event) {
			request := event.Args[0].(*FollowerCallbackRequest)
			dispatch := request.Dispatch
			newLeaderNode := request.NewLeaderNode
			newLeaderShardID := request.NewLeaderShardID

			// Transfer failed, stop transfer and reset state
			if result, err := dispatch.SendCloseEvent([]uint32{newLeaderShardID}, newLeaderNode); err != nil || !result {
				event.Cancel(errors.Wrap(err, EventTransferLeaderFailed))
			}
		},
	}
)

type LeaderCallbackRequest struct {
	Ctx      context.Context
	C        *cluster.Cluster
	Dispatch dispatch.EventDispatch

	OldLeaderNode    string
	OldLeaderShardID uint32
}

type FollowerCallbackRequest struct {
	Ctx      context.Context
	C        *cluster.Cluster
	Dispatch dispatch.EventDispatch

	NewLeaderNode    string
	NewLeaderShardID uint32
}

// NewShardFSM
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
func NewShardFSM(role clusterpb.ShardRole) *fsm.FSM {
	if role == clusterpb.ShardRole_LEADER {
		leaderShardFsm := fsm.NewFSM(
			StateLeader,
			leaderFsmEvent,
			leaderFsmCallbacks,
		)
		return leaderShardFsm
	}
	if role == clusterpb.ShardRole_FOLLOWER {
		followerShardFsm := fsm.NewFSM(
			StateFollower,
			followerFsmEvent,
			followerFsmCallbacks,
		)
		return followerShardFsm
	}
	return nil
}
