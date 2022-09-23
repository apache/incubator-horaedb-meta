// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package shard

import (
	"context"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/schedule"
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
			oldLeaderShardId := request.OldLeaderShardId
			// Update Etcd
			if clusterTopology, err := c.GetStorage().GetClusterTopology(ctx, c.GetClusterID()); err != nil {
				event.Cancel(errors.Wrap(err, EventPrepareTransferFollower))
			} else {
				shardViews := clusterTopology.ShardView
				for _, shard := range shardViews {
					if shard.GetId() == oldLeaderShardId {
						// shard.ShardRole = clusterpb.ShardRole_PENDING_FOLLOWER
					}
				}
				c.GetStorage().PutClusterTopology(ctx, c.GetClusterID(), c.GetMetaData().GetClusterTopology().Version, clusterTopology)
			}
		},
		EventTransferFollower: func(event *fsm.Event) {
			request := event.Args[0].(*LeaderCallbackRequest)

			ctx := request.Ctx
			handler := request.Handler
			oldLeaderNode := request.OldLeaderNode
			oldLeaderShardId := request.OldLeaderShardId

			if err := handler.Dispatch(ctx, oldLeaderNode, &schedule.CloseEvent{ShardIDs: []uint32{oldLeaderShardId}}); err != nil {
				event.Cancel(errors.Wrap(err, EventTransferFollower))
			}
		},
		EventTransferFollowerFailed: func(event *fsm.Event) {
			request := event.Args[0].(*LeaderCallbackRequest)

			ctx := request.Ctx
			handler := request.Handler
			oldLeaderNode := request.OldLeaderNode
			oldLeaderShardId := request.OldLeaderShardId

			// Transfer failed, stop transfer and reset state
			if err := handler.Dispatch(ctx, oldLeaderNode, &schedule.OpenEvent{ShardIDs: []uint32{oldLeaderShardId}}); err != nil {
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
			newLeaderShardId := request.NewLeaderShardId

			// Update Etcd
			if clusterTopology, err := c.GetStorage().GetClusterTopology(ctx, c.GetClusterID()); err != nil {
				event.Cancel(errors.Wrap(err, EventPrepareTransferLeader))
			} else {
				shardViews := clusterTopology.ShardView
				for _, shard := range shardViews {
					if shard.GetId() == newLeaderShardId {
						// shard.ShardRole = clusterpb.ShardRole_PENDING_LEADER
					}
				}
				c.GetStorage().PutClusterTopology(ctx, c.GetClusterID(), c.GetMetaData().GetClusterTopology().Version, clusterTopology)
			}
		},
		EventTransferLeader: func(event *fsm.Event) {
			request := event.Args[0].(*FollowerCallbackRequest)

			ctx := request.Ctx
			handler := request.Handler
			newLeaderNode := request.NewLeaderNode
			newLeaderShardId := request.NewLeaderShardId

			// Send event to CeresDB, waiting for response
			if err := handler.Dispatch(ctx, newLeaderNode, &schedule.OpenEvent{ShardIDs: []uint32{newLeaderShardId}}); err != nil {
				event.Cancel(errors.Wrap(err, EventTransferLeader))
			}
		},
		EventTransferLeaderFailed: func(event *fsm.Event) {
			request := event.Args[0].(*FollowerCallbackRequest)

			ctx := request.Ctx
			handler := request.Handler
			newLeaderNode := request.NewLeaderNode
			newLeaderShardId := request.NewLeaderShardId

			// Transfer failed, stop transfer and reset state
			if err := handler.Dispatch(ctx, newLeaderNode, &schedule.CloseEvent{ShardIDs: []uint32{newLeaderShardId}}); err != nil {
				event.Cancel(errors.Wrap(err, EventTransferLeaderFailed))
			}
		},
	}
)

type LeaderCallbackRequest struct {
	Ctx     context.Context
	C       *cluster.Cluster
	Handler *schedule.EventHandler

	OldLeaderNode    string
	OldLeaderShardId uint32
}

type FollowerCallbackRequest struct {
	Ctx     context.Context
	C       *cluster.Cluster
	Handler *schedule.EventHandler

	NewLeaderNode    string
	NewLeaderShardId uint32
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
