package procedure

import (
	"context"
	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/schedule"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
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
		{Name: EventTransferLeaderFailed, Src: []string{StateTransferLeaderWaiting}, Dst: StateTransferLeaderFailed},
	}
	transferLeaderCallbacks = fsm.Callbacks{
		EventTransferLeaderPrepare: func(event *fsm.Event) {
			request := event.Args[0].(*CallbackRequest)
			p := request.p
			c := request.c
			handler := request.handler
			ctx := request.cxt
			leaderFsm := request.leaderFsm
			followerFsm := request.followerFsm

			// When `Replication_Factor` == 1, newLeader is nil, find a suitable node and create a newFollower as newLeader
			if p.newLeader == nil {
				targetNode, err := c.GetClusterBalancer().SelectNode()
				if err != nil {
					event.Cancel(errors.WithMessage(err, "TransferLeaderProcedure start "))
				}
				// TODO: fix id alloc
				shardId := uint32(1)
				//shardId, err := c.allocShardID(ctx)
				//if err != nil {
				//	event.Cancel(errors.WithMessage(err, "TransferLeaderProcedure start "))
				//}
				newFollowerShard := &clusterpb.Shard{
					Id:        shardId,
					ShardRole: clusterpb.ShardRole_FOLLOWER,
					Node:      targetNode.GetMeta().Name,
				}
				if err := handler.Dispatch(ctx, targetNode.GetMeta().GetName(), &schedule.OpenEvent{ShardIDs: []uint32{shardId}}); err != nil {
					event.Cancel(errors.WithMessage(err, "TransferLeaderProcedure start "))
				}
				p.newLeader = newFollowerShard
			}

			leaderCallbackRequest := &cluster.LeaderCallbackRequest{
				Ctx:              ctx,
				C:                c,
				Handler:          handler,
				OldLeaderShardId: p.oldLeader.Id,
				OldLeaderNode:    p.oldLeader.Node,
			}

			followerCallbackRequest := &cluster.FollowerCallbackRequest{
				Ctx:              ctx,
				C:                c,
				Handler:          handler,
				NewLeaderShardId: p.newLeader.Id,
				NewLeaderNode:    p.newLeader.Node,
			}

			if err := leaderFsm.Event(cluster.EventPrepareTransferFollower, leaderCallbackRequest); err != nil {
				event.Cancel(errors.WithMessage(err, "TransferLeaderProcedure start "))
			}
			if err := followerFsm.Event(cluster.EventPrepareTransferLeader, followerCallbackRequest); err != nil {
				event.Cancel(errors.WithMessage(err, "TransferLeaderProcedure start "))
			}

			// Leader transfer first, follower wait until leader transfer finish
			if err := leaderFsm.Event(cluster.EventTransferFollower, leaderCallbackRequest); err != nil {
				event.Cancel(errors.WithMessage(err, "TransferLeaderProcedure start "))
			}
			if err := followerFsm.Event(cluster.EventTransferLeader, followerCallbackRequest); err != nil {
				event.Cancel(errors.WithMessage(err, "TransferLeaderProcedure start "))
			}

		},
		EventTransferLeaderFailed: func(event *fsm.Event) {
			request := event.Args[0].(*CallbackRequest)
			p := request.p
			c := request.c
			handler := request.handler
			ctx := request.cxt
			leaderFsm := request.leaderFsm
			followerFsm := request.followerFsm

			leaderCallbackRequest := &cluster.LeaderCallbackRequest{
				Ctx:              ctx,
				C:                c,
				Handler:          handler,
				OldLeaderShardId: p.oldLeader.Id,
				OldLeaderNode:    p.oldLeader.Node,
			}

			followerCallbackRequest := &cluster.FollowerCallbackRequest{
				Ctx:              ctx,
				C:                c,
				Handler:          handler,
				NewLeaderShardId: p.newLeader.Id,
				NewLeaderNode:    p.newLeader.Node,
			}

			leaderFsm.Event(cluster.EventTransferFollowerFailed, leaderCallbackRequest)
			followerFsm.Event(cluster.EventTransferLeaderFailed, followerCallbackRequest)
		},
		EventTransferLeaderSuccess: func(event *fsm.Event) {
			request := event.Args[0].(*CallbackRequest)
			p := request.p
			c := request.c
			ctx := request.cxt

			// Update cluster topology
			currentTopology := c.GetMetaData().GetClusterTopology()
			for i := 0; i < len(currentTopology.ShardView); i++ {
				shardId := currentTopology.ShardView[i].Id
				if shardId == p.oldLeader.Id {
					currentTopology.ShardView[i].ShardRole = clusterpb.ShardRole_FOLLOWER
				}
				if shardId == p.newLeader.Id {
					currentTopology.ShardView[i].ShardRole = clusterpb.ShardRole_LEADER
				}
			}

			if err := c.GetStorage().PutClusterTopology(ctx, c.GetClusterID(), c.GetMetaData().GetClusterTopology().Version, c.GetMetaData().GetClusterTopology()); err != nil {
				event.Cancel(errors.WithMessage(err, "TransferLeaderProcedure start "))
			}

			if err := c.Load(ctx); err != nil {
				event.Cancel(errors.WithMessage(err, "TransferLeaderProcedure start "))
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

// CallbackRequest is fsm callbacks request param
type CallbackRequest struct {
	p       *TransferLeaderProcedure
	c       *cluster.Cluster
	handler *schedule.EventHandler
	cxt     context.Context

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

func (p *TransferLeaderProcedure) Type() ShardOperationType {
	return ShardOperationTypeTransferLeader
}

func (p *TransferLeaderProcedure) Start(ctx context.Context, c *cluster.Cluster) error {
	p.state = StateRunning

	transferLeaderRequest := &CallbackRequest{
		p:           p,
		c:           c,
		cxt:         ctx,
		leaderFsm:   cluster.NewShardFSM(clusterpb.ShardRole_LEADER),
		followerFsm: cluster.NewShardFSM(clusterpb.ShardRole_FOLLOWER),
	}

	if err := p.fsm.Event(EventTransferLeaderPrepare, transferLeaderRequest); err != nil {
		err := p.fsm.Event(EventTransferLeaderFailed, transferLeaderRequest)
		p.state = StateFailed
		return errors.WithMessage(err, "coordinator transferLeaderShard")
	}

	if err := p.fsm.Event(EventTransferLeaderSuccess, transferLeaderRequest); err != nil {
		return errors.WithMessage(err, "coordinator transferLeaderShard")
	}

	p.state = StateFinished
	return nil
}

func (p *TransferLeaderProcedure) Cancel(ctx context.Context) error {
	p.state = StateCancelled
	return nil
}

func (p *TransferLeaderProcedure) State() State {
	return p.state
}
