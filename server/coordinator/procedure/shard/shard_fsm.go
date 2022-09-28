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
		EventPrepareTransferFollower: prepareTransferFollowerCallback,
		EventTransferFollower:        transferFollowerCallback,
		EventTransferFollowerFailed:  transferFollowerFailedCallback,
	}
)

var (
	followerFsmEvent = fsm.Events{
		{Name: EventPrepareTransferLeader, Src: []string{StateFollower}, Dst: StatePendingLeader},
		{Name: EventTransferLeader, Src: []string{StatePendingLeader}, Dst: StateLeader},
		{Name: EventTransferLeaderFailed, Src: []string{StatePendingLeader}, Dst: StateFollower},
	}
	followerFsmCallbacks = fsm.Callbacks{
		EventPrepareTransferLeader: prepareTransferLeaderCallback,
		EventTransferLeader:        transferLeaderCallback,
		EventTransferLeaderFailed:  transferLeaderFailed,
	}
)

type LeaderCallbackRequest struct {
	Ctx            context.Context
	C              *cluster.Cluster
	ActionDispatch dispatch.ActionDispatch

	OldLeaderNode    string
	OldLeaderShardID uint32
}

type FollowerCallbackRequest struct {
	Ctx            context.Context
	C              *cluster.Cluster
	ActionDispatch dispatch.ActionDispatch

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
	switch role {
	case clusterpb.ShardRole_LEADER:
		leaderShardFsm := fsm.NewFSM(
			StateLeader,
			leaderFsmEvent,
			leaderFsmCallbacks,
		)
		return leaderShardFsm
	case clusterpb.ShardRole_FOLLOWER:
		followerShardFsm := fsm.NewFSM(
			StateFollower,
			followerFsmEvent,
			followerFsmCallbacks,
		)
		return followerShardFsm
	}
	return nil
}

func prepareTransferFollowerCallback(event *fsm.Event) {
	request := event.Args[0].(*LeaderCallbackRequest)
	ctx := request.Ctx
	c := request.C
	oldLeaderShardID := request.OldLeaderShardID

	// TODO: add ShardRole_PENDING_FOLLOWER enum, replace ShardRole_LEADER to ShardRole_PENDING_FOLLOWER
	if err := updateShardRole(ctx, c, oldLeaderShardID, clusterpb.ShardRole_FOLLOWER); err != nil {
		event.Cancel(errors.Wrap(err, EventPrepareTransferFollower))
	}
}

func transferFollowerCallback(event *fsm.Event) {
	request := event.Args[0].(*LeaderCallbackRequest)
	ctx := request.Ctx
	c := request.C
	actionDispatch := request.ActionDispatch
	oldLeaderNode := request.OldLeaderNode
	oldLeaderShardID := request.OldLeaderShardID

	action := dispatch.CloseShardAction{ShardIDs: []uint32{oldLeaderShardID}}
	if err := actionDispatch.CloseShards(ctx, oldLeaderNode, action); err != nil {
		event.Cancel(errors.Wrap(err, EventPrepareTransferFollower))
	}

	if err := updateShardRole(ctx, c, oldLeaderShardID, clusterpb.ShardRole_FOLLOWER); err != nil {
		event.Cancel(errors.Wrap(err, EventTransferFollower))
	}
}

func transferFollowerFailedCallback(event *fsm.Event) {
	request := event.Args[0].(*LeaderCallbackRequest)
	ctx := request.Ctx
	c := request.C
	actionDispatch := request.ActionDispatch
	oldLeaderNode := request.OldLeaderNode
	oldLeaderShardID := request.OldLeaderShardID

	// Transfer failed, stop transfer and reset state
	action := dispatch.OpenShardAction{ShardIDs: []uint32{oldLeaderShardID}}
	if err := actionDispatch.OpenShards(ctx, oldLeaderNode, action); err != nil {
		event.Cancel(errors.Wrap(err, EventTransferFollowerFailed))
	}

	if err := updateShardRole(ctx, c, oldLeaderShardID, clusterpb.ShardRole_LEADER); err != nil {
		event.Cancel(errors.Wrap(err, EventTransferFollowerFailed))
	}
}

func prepareTransferLeaderCallback(event *fsm.Event) {
	request := event.Args[0].(*FollowerCallbackRequest)
	ctx := request.Ctx
	c := request.C
	newLeaderShardID := request.NewLeaderShardID

	// TODO: add ShardRole_PENDING_LEADER enum, replace ShardRole_LEADER to ShardRole_PENDING_LEADER
	if err := updateShardRole(ctx, c, newLeaderShardID, clusterpb.ShardRole_LEADER); err != nil {
		event.Cancel(errors.Wrap(err, EventPrepareTransferLeader))
	}
}

func transferLeaderCallback(event *fsm.Event) {
	request := event.Args[0].(*FollowerCallbackRequest)
	ctx := request.Ctx
	actionDispatch := request.ActionDispatch
	c := request.C
	newLeaderNode := request.NewLeaderNode
	newLeaderShardID := request.NewLeaderShardID

	// Send event to CeresDB, waiting for response
	action := dispatch.OpenShardAction{ShardIDs: []uint32{newLeaderShardID}}
	if err := actionDispatch.OpenShards(ctx, newLeaderNode, action); err != nil {
		event.Cancel(errors.Wrap(err, EventTransferLeader))
	}

	if err := updateShardRole(ctx, c, newLeaderShardID, clusterpb.ShardRole_LEADER); err != nil {
		event.Cancel(errors.Wrap(err, EventPrepareTransferLeader))
	}
}

func transferLeaderFailed(event *fsm.Event) {
	request := event.Args[0].(*FollowerCallbackRequest)
	ctx := request.Ctx
	actionDispatch := request.ActionDispatch
	c := request.C
	newLeaderNode := request.NewLeaderNode
	newLeaderShardID := request.NewLeaderShardID

	// Transfer failed, stop transfer and reset state
	action := dispatch.CloseShardAction{ShardIDs: []uint32{newLeaderShardID}}
	if err := actionDispatch.CloseShards(ctx, newLeaderNode, action); err != nil {
		event.Cancel(errors.Wrap(err, EventTransferLeaderFailed))
	}

	if err := updateShardRole(ctx, c, newLeaderShardID, clusterpb.ShardRole_FOLLOWER); err != nil {
		event.Cancel(errors.Wrap(err, EventPrepareTransferLeader))
	}
}

func updateShardRole(ctx context.Context, c *cluster.Cluster, oldLeaderShardID uint32, role clusterpb.ShardRole) error {
	shardViews, err := c.GetClusterShardView()
	if err != nil {
		return errors.WithMessage(err, "updateShardRole failed")
	}
	for _, shard := range shardViews {
		// nolint
		if shard.GetId() == oldLeaderShardID {
			shard.ShardRole = role
		}
	}
	if err := c.UpdateClusterTopology(ctx, c.GetClusterState(), shardViews); err != nil {
		return errors.WithMessage(err, "updateShardRole failed")
	}
	return nil
}
