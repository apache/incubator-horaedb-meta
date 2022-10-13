// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"fmt"
	"sync"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	eventTransferLeaderPrepare = "EventTransferLeaderPrepare"
	eventTransferLeaderFailed  = "EventTransferLeaderFailed"
	eventTransferLeaderSuccess = "EventTransferLeaderSuccess"

	stateTransferLeaderBegin   = "StateTransferLeaderBegin"
	stateTransferLeaderWaiting = "StateTransferLeaderWaiting"
	stateTransferLeaderFinish  = "StateTransferLeaderFinish"
	stateTransferLeaderFailed  = "StateTransferLeaderFailed"
)

var (
	transferLeaderEvents = fsm.Events{
		{Name: eventTransferLeaderPrepare, Src: []string{stateTransferLeaderBegin}, Dst: stateTransferLeaderWaiting},
		{Name: eventTransferLeaderSuccess, Src: []string{stateTransferLeaderWaiting}, Dst: stateTransferLeaderFinish},
		{Name: eventTransferLeaderFailed, Src: []string{stateTransferLeaderWaiting}, Dst: stateTransferLeaderFailed},
	}
	transferLeaderCallbacks = fsm.Callbacks{
		eventTransferLeaderPrepare: transferLeaderPrepareCallback,
		eventTransferLeaderFailed:  transferLeaderFailedCallback,
		eventTransferLeaderSuccess: transferLeaderSuccessCallback,
	}
)

type TransferLeaderProcedure struct {
	// Protect the state.
	lock  sync.RWMutex
	state State

	fsm       *fsm.FSM
	id        uint64
	dispatch  eventdispatch.Dispatch
	cluster   *cluster.Cluster
	oldLeader *clusterpb.Shard
	newLeader *clusterpb.Shard
}

// TransferLeaderCallbackRequest is fsm callbacks param
type TransferLeaderCallbackRequest struct {
	cluster  *cluster.Cluster
	ctx      context.Context
	dispatch eventdispatch.Dispatch

	oldLeader *clusterpb.Shard
	newLeader *clusterpb.Shard
}

func NewTransferLeaderProcedure(dispatch eventdispatch.Dispatch, cluster *cluster.Cluster, oldLeader *clusterpb.Shard, newLeader *clusterpb.Shard, id uint64) Procedure {
	transferLeaderOperationFsm := fsm.NewFSM(
		stateTransferLeaderBegin,
		transferLeaderEvents,
		transferLeaderCallbacks,
	)

	return &TransferLeaderProcedure{fsm: transferLeaderOperationFsm, dispatch: dispatch, cluster: cluster, id: id, state: StateInit, oldLeader: oldLeader, newLeader: newLeader}
}

func (p *TransferLeaderProcedure) ID() uint64 {
	return p.id
}

func (p *TransferLeaderProcedure) Typ() Typ {
	return TransferLeader
}

func (p *TransferLeaderProcedure) Start(ctx context.Context) error {
	p.UpdateStateWithLock(StateRunning)

	transferLeaderRequest := &TransferLeaderCallbackRequest{
		cluster:   p.cluster,
		ctx:       ctx,
		newLeader: p.newLeader,
		oldLeader: p.oldLeader,
		dispatch:  p.dispatch,
	}

	if err := p.fsm.Event(eventTransferLeaderPrepare, transferLeaderRequest); err != nil {
		err := p.fsm.Event(eventTransferLeaderFailed, transferLeaderRequest)
		p.UpdateStateWithLock(StateFailed)
		return errors.WithMessage(err, "coordinator transferLeaderShard start")
	}

	if err := p.fsm.Event(eventTransferLeaderSuccess, transferLeaderRequest); err != nil {
		return errors.WithMessage(err, "coordinator transferLeaderShard start")
	}

	p.UpdateStateWithLock(StateFinished)
	return nil
}

func (p *TransferLeaderProcedure) Cancel(_ context.Context) error {
	p.UpdateStateWithLock(StateCancelled)
	return nil
}

func (p *TransferLeaderProcedure) State() State {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.state
}

func transferLeaderPrepareCallback(event *fsm.Event) {
	request := event.Args[0].(*TransferLeaderCallbackRequest)
	ctx := request.ctx

	closeShardRequest := &eventdispatch.CloseShardRequest{
		ShardID: request.oldLeader.Id,
	}
	if err := request.dispatch.CloseShard(ctx, request.oldLeader.Node, closeShardRequest); err != nil {
		event.Cancel(errors.WithMessage(err, "coordinator transferLeaderShard prepare callback, close shard failed"))
		log.Error("coordinator transferLeaderShard prepare callback, close shard failed", zap.Uint32("shardId", request.oldLeader.Id))
		return
	}

	openShardRequest := &eventdispatch.OpenShardRequest{
		Shard: &cluster.ShardInfo{ShardID: request.newLeader.Id, ShardRole: clusterpb.ShardRole_LEADER},
	}
	if err := request.dispatch.OpenShard(ctx, request.newLeader.Node, openShardRequest); err != nil {
		event.Cancel(errors.WithMessage(err, "coordinator transferLeaderShard prepare callback, open shard failed"))
		log.Error("coordinator transferLeaderShard prepare callback, open shard failed", zap.Uint32("shardId", request.newLeader.Id))
		return
	}
}

func transferLeaderFailedCallback(_ *fsm.Event) {
	// TODO: Use RollbackProcedure to rollback transfer failed
}

func transferLeaderSuccessCallback(event *fsm.Event) {
	request := event.Args[0].(*TransferLeaderCallbackRequest)
	c := request.cluster
	ctx := request.ctx

	// Update cluster topology
	shardView, err := c.GetClusterShardView()
	if err != nil {
		event.Cancel(errors.WithMessage(err, "TransferLeaderProcedure success callback"))
		return
	}
	var oldLeaderIndex = -1
	for i := 0; i < len(shardView); i++ {
		shardID := shardView[i].Id
		if shardID == request.oldLeader.Id {
			oldLeaderIndex = i
		}
	}
	if oldLeaderIndex == -1 {
		event.Cancel(errors.WithMessage(cluster.ErrShardNotFound, fmt.Sprintf("shard not found,shardID = %d", request.oldLeader.Id)))
	}
	shardView = append(shardView[:oldLeaderIndex], shardView[oldLeaderIndex+1:]...)
	shardView = append(shardView, request.newLeader)

	if err := c.UpdateClusterTopology(ctx, c.GetClusterState(), shardView); err != nil {
		event.Cancel(errors.WithMessage(err, "TransferLeaderProcedure start success callback"))
		log.Error("coordinator transferLeaderShard prepare callback, update shard topology failed")
		return
	}
}

func (p *TransferLeaderProcedure) UpdateStateWithLock(state State) {
	p.lock.Lock()
	p.state = state
	p.lock.Unlock()
}
