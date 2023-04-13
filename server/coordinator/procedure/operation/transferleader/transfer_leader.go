// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package transferleader

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Fsm state change: Begin -> CloseOldLeader -> OpenNewLeader -> Finish.
// CloseOldLeader will send close shard request if the old leader node exists.
// OpenNewLeader will send open shard request to new leader node.
const (
	eventCloseOldLeader = "EventCloseOldLeader"
	eventOpenNewLeader  = "EventOpenNewLeader"
	eventFinish         = "EventFinish"

	stateBegin          = "StateBegin"
	stateCloseOldLeader = "StateCloseOldLeader"
	stateOpenNewLeader  = "StateOpenNewLeader"
	stateFinish         = "StateFinish"
)

var (
	transferLeaderEvents = fsm.Events{
		{Name: eventCloseOldLeader, Src: []string{stateBegin}, Dst: stateCloseOldLeader},
		{Name: eventOpenNewLeader, Src: []string{stateCloseOldLeader}, Dst: stateOpenNewLeader},
		{Name: eventFinish, Src: []string{stateOpenNewLeader}, Dst: stateFinish},
	}
	transferLeaderCallbacks = fsm.Callbacks{
		eventCloseOldLeader: closeOldLeaderCallback,
		eventOpenNewLeader:  openNewShardCallback,
		eventFinish:         finishCallback,
	}
)

type Procedure struct {
	id  uint64
	fsm *fsm.FSM

	dispatch eventdispatch.Dispatch
	storage  procedure.Storage

	relatedVersionInfo procedure.RelatedVersionInfo
	snapShot           metadata.Snapshot
	shardID            storage.ShardID
	oldLeaderNodeName  string
	newLeaderNodeName  string

	// Protect the state.
	lock  sync.RWMutex
	state procedure.State
}

// rawData used for storage, procedure will be converted to persist raw data before saved in storage.
type rawData struct {
	ID       uint64
	FsmState string
	State    procedure.State

	snapShot          metadata.Snapshot
	ShardID           storage.ShardID
	OldLeaderNodeName string
	NewLeaderNodeName string
}

// callbackRequest is fsm callbacks param.
type callbackRequest struct {
	ctx context.Context
	p   *Procedure
}

type ProcedureParams struct {
	ID uint64

	Dispatch eventdispatch.Dispatch
	Storage  procedure.Storage

	ClusterSnapShot metadata.Snapshot

	ShardID           storage.ShardID
	OldLeaderNodeName string
	NewLeaderNodeName string
}

func NewProcedure(params ProcedureParams) (procedure.Procedure, error) {
	if err := validClusterTopology(params.ClusterSnapShot.Topology, params.ShardID, params.OldLeaderNodeName); err != nil {
		return nil, err
	}

	relatedVersionInfo := buildRelatedVersionInfo(params)

	transferLeaderOperationFsm := fsm.NewFSM(
		stateBegin,
		transferLeaderEvents,
		transferLeaderCallbacks,
	)

	return &Procedure{
		id:                 params.ID,
		fsm:                transferLeaderOperationFsm,
		dispatch:           params.Dispatch,
		relatedVersionInfo: relatedVersionInfo,
		snapShot:           params.ClusterSnapShot,
		storage:            params.Storage,
		shardID:            params.ShardID,
		oldLeaderNodeName:  params.OldLeaderNodeName,
		newLeaderNodeName:  params.NewLeaderNodeName,
		state:              procedure.StateInit,
	}, nil
}

func buildRelatedVersionInfo(params ProcedureParams) procedure.RelatedVersionInfo {
	shardViewWithVersion := make(map[storage.ShardID]uint64, 0)
	for _, shardView := range params.ClusterSnapShot.Topology.ShardViews {
		if shardView.ShardID == params.ShardID {
			shardViewWithVersion[params.ShardID] = shardView.Version
		}
	}
	relatedVersionInfo := procedure.RelatedVersionInfo{
		ClusterID:        params.ClusterSnapShot.Topology.ClusterView.ClusterID,
		ShardWithVersion: shardViewWithVersion,
		ClusterVersion:   params.ClusterSnapShot.Topology.ClusterView.Version,
	}
	return relatedVersionInfo
}

func validClusterTopology(topology metadata.Topology, shardID storage.ShardID, oldLeaderNodeName string) error {
	shardNodes := topology.ClusterView.ShardNodes
	if len(shardNodes) == 0 {
		log.Error("shard not exist in any node", zap.Uint32("shardID", uint32(shardID)))
		return metadata.ErrShardNotFound
	}
	for _, shardNode := range shardNodes {
		if shardNode.ShardRole == storage.ShardRoleLeader {
			leaderNodeName := shardNode.NodeName
			if leaderNodeName != oldLeaderNodeName {
				log.Error("shard leader node not match", zap.String("requestOldLeaderNodeName", oldLeaderNodeName), zap.String("actualOldLeaderNodeName", leaderNodeName))
				return metadata.ErrNodeNotFound
			}
		}
	}
	found := false
	for _, shardView := range topology.ShardViews {
		if shardView.ShardID == shardID {
			found = true
		}
	}
	if !found {
		log.Error("shard not found", zap.Uint64("shardID", uint64(shardID)))
		return metadata.ErrShardNotFound
	}
	return nil
}

func (p *Procedure) ID() uint64 {
	return p.id
}

func (p *Procedure) Typ() procedure.Typ {
	return procedure.TransferLeader
}

func (p *Procedure) RelatedVersionInfo() procedure.RelatedVersionInfo {
	return p.relatedVersionInfo
}

func (p *Procedure) Priority() procedure.Priority {
	return procedure.PriorityHigh
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateStateWithLock(procedure.StateRunning)

	transferLeaderRequest := callbackRequest{
		ctx: ctx,
		p:   p,
	}

	for {
		switch p.fsm.Current() {
		case stateBegin:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "transferLeader procedure persist")
			}
			if err := p.fsm.Event(eventCloseOldLeader, transferLeaderRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "transferLeader procedure close old leader")
			}
		case stateCloseOldLeader:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "transferLeader procedure persist")
			}
			if err := p.fsm.Event(eventOpenNewLeader, transferLeaderRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "transferLeader procedure open new leader")
			}
		case stateOpenNewLeader:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "transferLeader procedure persist")
			}
			if err := p.fsm.Event(eventFinish, transferLeaderRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "transferLeader procedure finish")
			}
		case stateFinish:
			// TODO: The state update sequence here is inconsistent with the previous one. Consider reconstructing the state update logic of the state machine.
			p.updateStateWithLock(procedure.StateFinished)
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "transferLeader procedure persist")
			}
			return nil
		}
	}
}

func (p *Procedure) persist(ctx context.Context) error {
	meta, err := p.convertToMeta()
	if err != nil {
		return errors.WithMessage(err, "convert to meta")
	}
	err = p.storage.CreateOrUpdate(ctx, meta)
	if err != nil {
		return errors.WithMessage(err, "createOrUpdate procedure storage")
	}
	return nil
}

func (p *Procedure) Cancel(_ context.Context) error {
	p.updateStateWithLock(procedure.StateCancelled)
	return nil
}

func (p *Procedure) State() procedure.State {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.state
}

func closeOldLeaderCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	ctx := req.ctx

	closeShardRequest := eventdispatch.CloseShardRequest{
		ShardID: uint32(req.p.shardID),
	}
	if err := req.p.dispatch.CloseShard(ctx, req.p.oldLeaderNodeName, closeShardRequest); err != nil {
		procedure.CancelEventWithLog(event, err, "close shard", zap.Uint32("shardID", uint32(req.p.shardID)), zap.String("oldLeaderName", req.p.oldLeaderNodeName))
		return
	}
}

func openNewShardCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	ctx := req.ctx

	var preVersion uint64
	for _, shardView := range req.p.snapShot.Topology.ShardViews {
		if req.p.shardID == shardView.ShardID {
			preVersion = shardView.Version
		}
	}

	openShardRequest := eventdispatch.OpenShardRequest{
		Shard: metadata.ShardInfo{ID: req.p.shardID, Role: storage.ShardRoleLeader, Version: preVersion + 1},
	}

	if err := req.p.dispatch.OpenShard(ctx, req.p.newLeaderNodeName, openShardRequest); err != nil {
		procedure.CancelEventWithLog(event, err, "open shard", zap.Uint32("shardID", uint32(req.p.shardID)), zap.String("newLeaderNode", req.p.newLeaderNodeName))
		return
	}
}

func finishCallback(event *fsm.Event) {
	request, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}

	log.Info("transfer leader finish", zap.Uint32("shardID", uint32(request.p.shardID)), zap.String("oldLeaderNode", request.p.oldLeaderNodeName), zap.String("newLeaderNode", request.p.newLeaderNodeName))
}

func (p *Procedure) updateStateWithLock(state procedure.State) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.state = state
}

// TODO: Consider refactor meta procedure convertor function, encapsulate as a tool function.
func (p *Procedure) convertToMeta() (procedure.Meta, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	rawData := rawData{
		ID:                p.id,
		FsmState:          p.fsm.Current(),
		ShardID:           p.shardID,
		snapShot:          p.snapShot,
		OldLeaderNodeName: p.oldLeaderNodeName,
		NewLeaderNodeName: p.newLeaderNodeName,
		State:             p.state,
	}
	rawDataBytes, err := json.Marshal(rawData)
	if err != nil {
		return procedure.Meta{}, procedure.ErrEncodeRawData.WithCausef("marshal raw data, procedureID:%v, err:%v", p.shardID, err)
	}

	meta := procedure.Meta{
		ID:    p.id,
		Typ:   procedure.TransferLeader,
		State: p.state,

		RawData: rawDataBytes,
	}

	return meta, nil
}
