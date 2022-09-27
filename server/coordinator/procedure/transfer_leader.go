// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"sort"
	"time"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"go.uber.org/zap"

	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/dispatch"

	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/shard"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
)

const (
	MaxLockRetrySize = 3
	LockWaitDuration = time.Second * 1
)

const (
	EventTransferLeaderPrepare = "EventTransferLeaderPrepare"
	EventTransferLeaderFailed  = "EventTransferLeaderFailed"
	EventTransferLeaderSuccess = "EventTransferLeaderSuccess"

	StateTransferLeaderBegin   = "StateTransferLeaderBegin"
	StateTransferLeaderWaiting = "StateTransferLeaderWaiting"
	StateTransferLeaderFinish  = "StateTransferLeaderFinish"
	StateTransferLeaderFailed  = "StateTransferLeaderFailed"
)

var (
	transferLeaderEvents = fsm.Events{
		{Name: EventTransferLeaderPrepare, Src: []string{StateTransferLeaderBegin}, Dst: StateTransferLeaderWaiting},
		{Name: EventTransferLeaderSuccess, Src: []string{StateTransferLeaderWaiting}, Dst: StateTransferLeaderFinish},
		{Name: EventTransferLeaderFailed, Src: []string{StateTransferLeaderBegin, StateTransferLeaderWaiting}, Dst: StateTransferLeaderFailed},
	}
	transferLeaderCallbacks = fsm.Callbacks{
		EventTransferLeaderPrepare: func(event *fsm.Event) {
			request := event.Args[0].(*TransferLeaderCallbackRequest)
			p := request.p
			c := request.c
			dispatch := request.dispatch
			ctx := request.cxt
			leaderFsm := request.leaderFsm
			followerFsm := request.followerFsm

			leaderCallbackRequest := &shard.LeaderCallbackRequest{
				Ctx:              ctx,
				C:                c,
				Dispatch:         dispatch,
				OldLeaderShardID: p.oldLeader.Id,
				OldLeaderNode:    p.oldLeader.Node,
			}

			followerCallbackRequest := &shard.FollowerCallbackRequest{
				Ctx:              ctx,
				C:                c,
				Dispatch:         dispatch,
				NewLeaderShardID: p.newLeader.Id,
				NewLeaderNode:    p.newLeader.Node,
			}

			if err := leaderFsm.Event(shard.EventPrepareTransferFollower, leaderCallbackRequest); err != nil {
				event.Cancel(errors.WithMessage(err, "TransferLeaderProcedure start"))
			}
			if err := followerFsm.Event(shard.EventPrepareTransferLeader, followerCallbackRequest); err != nil {
				event.Cancel(errors.WithMessage(err, "TransferLeaderProcedure start"))
			}

			// Leader transfer first, follower wait until leader transfer finish
			if err := leaderFsm.Event(shard.EventTransferFollower, leaderCallbackRequest); err != nil {
				event.Cancel(errors.WithMessage(err, "TransferLeaderProcedure start"))
			}
			if err := followerFsm.Event(shard.EventTransferLeader, followerCallbackRequest); err != nil {
				event.Cancel(errors.WithMessage(err, "TransferLeaderProcedure start"))
			}
		},
		EventTransferLeaderFailed: func(event *fsm.Event) {
			request := event.Args[0].(*TransferLeaderCallbackRequest)
			p := request.p
			c := request.c
			dispatch := request.dispatch
			ctx := request.cxt
			leaderFsm := request.leaderFsm
			followerFsm := request.followerFsm

			leaderCallbackRequest := &shard.LeaderCallbackRequest{
				Ctx:              ctx,
				C:                c,
				Dispatch:         dispatch,
				OldLeaderShardID: p.oldLeader.Id,
				OldLeaderNode:    p.oldLeader.Node,
			}

			followerCallbackRequest := &shard.FollowerCallbackRequest{
				Ctx:              ctx,
				C:                c,
				Dispatch:         dispatch,
				NewLeaderShardID: p.newLeader.Id,
				NewLeaderNode:    p.newLeader.Node,
			}

			if err := leaderFsm.Event(shard.EventTransferFollowerFailed, leaderCallbackRequest); err != nil {
				log.Error("transfer follower rollback failed", zap.Error(err))
			}
			if err := followerFsm.Event(shard.EventTransferLeaderFailed, followerCallbackRequest); err != nil {
				log.Error("transfer leader rollback failed", zap.Error(err))
			}
		},
		EventTransferLeaderSuccess: func(event *fsm.Event) {
			request := event.Args[0].(*TransferLeaderCallbackRequest)
			p := request.p
			c := request.c
			ctx := request.cxt

			// Update cluster topology.
			shardView := c.GetClusterShardView()
			for i := 0; i < len(shardView); i++ {
				shardID := shardView[i].Id
				if shardID == p.oldLeader.Id {
					shardView[i].ShardRole = clusterpb.ShardRole_FOLLOWER
				}
				if shardID == p.newLeader.Id {
					shardView[i].ShardRole = clusterpb.ShardRole_LEADER
				}
			}

			if err := c.UpdateClusterTopology(ctx, c.GetClusterState(), shardView); err != nil {
				event.Cancel(errors.WithMessage(err, "TransferLeaderProcedure start"))
			}

			if err := c.Load(ctx); err != nil {
				event.Cancel(errors.WithMessage(err, "TransferLeaderProcedure start"))
			}
		},
	}
)

type TransferLeaderProcedure struct {
	id    uint64
	state State

	fsm       *fsm.FSM
	oldLeader *clusterpb.Shard
	newLeader *clusterpb.Shard
	c         *cluster.Cluster
}

// TransferLeaderCallbackRequest is fsm callbacks request param.
type TransferLeaderCallbackRequest struct {
	p        *TransferLeaderProcedure
	c        *cluster.Cluster
	dispatch dispatch.EventDispatch
	cxt      context.Context

	leaderFsm   *fsm.FSM
	followerFsm *fsm.FSM
}

func NewTransferLeaderProcedure(cluster *cluster.Cluster, oldLeader *clusterpb.Shard, newLeader *clusterpb.Shard) *TransferLeaderProcedure {
	transferLeaderOperationFsm := fsm.NewFSM(
		StateTransferLeaderBegin,
		transferLeaderEvents,
		transferLeaderCallbacks,
	)

	// TODO: fix id alloc
	id := uint64(1)
	return &TransferLeaderProcedure{fsm: transferLeaderOperationFsm, id: id, state: StateInit, oldLeader: oldLeader, newLeader: newLeader, c: cluster}
}

func (p *TransferLeaderProcedure) ID() uint64 {
	return p.id
}

func (p *TransferLeaderProcedure) Type() Type {
	return TransferLeader
}

func (p *TransferLeaderProcedure) Start(ctx context.Context) error {
	p.state = StateRunning

	// Lock shard. To avoid deadlock, lock according ID from small to large.
	shardIDs := []uint32{p.newLeader.Id, p.oldLeader.Id}
	sort.Slice(shardIDs, func(i, j int) bool { return shardIDs[i] < shardIDs[j] })
	for _, id := range shardIDs {
		lockResult := p.c.LockShardByIDWithRetry(id, MaxLockRetrySize, LockWaitDuration)
		if !lockResult {
			return ErrLockShard.WithCausef("lock shard failed, ShardID=%d ,MaxLockRetrySize=%d, LockWaitDuration=%s", id, MaxLockRetrySize, LockWaitDuration)
		}
	}
	transferLeaderRequest := &TransferLeaderCallbackRequest{
		p:           p,
		c:           p.c,
		dispatch:    dispatch.NewEventDispatchImpl(),
		cxt:         ctx,
		leaderFsm:   shard.NewShardFSM(clusterpb.ShardRole_LEADER),
		followerFsm: shard.NewShardFSM(clusterpb.ShardRole_FOLLOWER),
	}

	if err := p.fsm.Event(EventTransferLeaderPrepare, transferLeaderRequest); err != nil {
		p.state = StateFailed
		if err := p.fsm.Event(EventTransferLeaderFailed, transferLeaderRequest); err != nil {
			// TODO: EventTransferLeaderFailed event failed, how to process rollback invalid?
			return errors.WithMessage(err, "coordinator transferLeaderShard")
		}
		return errors.WithMessage(err, "coordinator transferLeaderShard")
	}

	if err := p.fsm.Event(EventTransferLeaderSuccess, transferLeaderRequest); err != nil {
		return errors.WithMessage(err, "coordinator transferLeaderShard")
	}

	// Unlock shard in reverse order.
	for i := len(shardIDs) - 1; i >= 0; i-- {
		ID := shardIDs[i]
		p.c.UnlockShardByID(ID)
	}

	p.state = StateFinished
	return nil
}

func (p *TransferLeaderProcedure) Cancel(ctx context.Context) error {
	p.state = StateCancelled
	// TODO: Add rollback implementation
	return nil
}

func (p *TransferLeaderProcedure) State() State {
	return p.state
}
