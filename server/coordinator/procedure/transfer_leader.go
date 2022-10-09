// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"sync"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/dispatch"
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
		EventTransferLeaderPrepare: transferLeaderPrepareCallback,
		EventTransferLeaderFailed:  transferLeaderFailedCallback,
		EventTransferLeaderSuccess: transferLeaderSuccessCallback,
	}
)

type TransferLeaderProcedure struct {
	lock     sync.RWMutex
	fsm      *fsm.FSM
	id       uint64
	state    State
	dispatch dispatch.ActionDispatch
	cluster  *cluster.Cluster

	oldLeader *clusterpb.Shard
	newLeader *clusterpb.Shard
}

// TransferLeaderCallbackRequest is fsm callbacks param
type TransferLeaderCallbackRequest struct {
	cluster  *cluster.Cluster
	cxt      context.Context
	dispatch dispatch.ActionDispatch

	oldLeader *clusterpb.Shard
	newLeader *clusterpb.Shard
}

func NewTransferLeaderProcedure(oldLeader *clusterpb.Shard, newLeader *clusterpb.Shard) Procedure {
	transferLeaderOperationFsm := fsm.NewFSM(
		StateTransferLeaderBegin,
		transferLeaderEvents,
		transferLeaderCallbacks,
	)
	// alloc id
	id := uint64(1)

	return &TransferLeaderProcedure{fsm: transferLeaderOperationFsm, id: id, state: StateInit, oldLeader: oldLeader, newLeader: newLeader}
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
		cxt:       ctx,
		newLeader: p.newLeader,
		oldLeader: p.oldLeader,
		dispatch:  p.dispatch,
	}

	if err := p.fsm.Event(EventTransferLeaderPrepare, transferLeaderRequest); err != nil {
		err := p.fsm.Event(EventTransferLeaderFailed, transferLeaderRequest)
		p.UpdateStateWithLock(StateFailed)
		return errors.WithMessage(err, "coordinator transferLeaderShard start")
	}

	if err := p.fsm.Event(EventTransferLeaderSuccess, transferLeaderRequest); err != nil {
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
	cxt := request.cxt
	oldLeader := request.oldLeader
	newLeader := request.newLeader

	closeShardAction := dispatch.CloseShardAction{
		ShardIDs: []uint32{oldLeader.Id},
	}
	if err := request.dispatch.CloseShards(cxt, oldLeader.Node, closeShardAction); err != nil {
		event.Cancel(errors.WithMessage(err, "coordinator transferLeaderShard prepare callback"))
	}

	openShardAction := dispatch.OpenShardAction{
		ShardIDs: []uint32{newLeader.Id},
	}
	if err := request.dispatch.OpenShards(cxt, newLeader.Node, openShardAction); err != nil {
		event.Cancel(errors.WithMessage(err, "coordinator transferLeaderShard prepare callback"))
	}
}

func transferLeaderFailedCallback(_ *fsm.Event) {
	// TODO: Use RollbackProcedure to rollback transfer failed
}

func transferLeaderSuccessCallback(event *fsm.Event) {
	request := event.Args[0].(*TransferLeaderCallbackRequest)
	cluster := request.cluster
	ctx := request.cxt
	oldLeader := request.oldLeader
	newLeader := request.newLeader

	// Update cluster topology
	shardView, err := cluster.GetClusterShardView()
	if err != nil {
		event.Cancel(errors.WithMessage(err, "TransferLeaderProcedure success callback"))
	}
	for i := 0; i < len(shardView); i++ {
		shardID := shardView[i].Id
		if shardID == oldLeader.Id {
			shardView[i].ShardRole = clusterpb.ShardRole_FOLLOWER
		}
		if shardID == newLeader.Id {
			shardView[i].ShardRole = clusterpb.ShardRole_LEADER
		}
	}

	if err := cluster.UpdateClusterTopology(ctx, cluster.GetClusterState(), shardView); err != nil {
		event.Cancel(errors.WithMessage(err, "TransferLeaderProcedure start success callback"))
	}
}

func (p *TransferLeaderProcedure) UpdateStateWithLock(state State) {
	p.lock.Lock()
	p.state = state
	p.lock.Unlock()
}
