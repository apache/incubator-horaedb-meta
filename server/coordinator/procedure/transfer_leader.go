// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Begin -> UpdateMetadata -> CloseOldLeader -> OpenNewLeader -> Finish
const (
	eventTransferLeaderUpdateMetadata = "EventTransferLeaderUpdateMetadata"
	eventTransferLeaderCloseOldLeader = "EventTransferLeaderCloseOldLeader"
	eventTransferLeaderOpenNewLeader  = "EventTransferLeaderOpenNewLeader"
	eventTransferLeaderFinish         = "EventTransferLeaderFinish"

	stateTransferLeaderBegin          = "StateTransferLeaderBegin"
	stateTransferLeaderUpdateMetadata = "StateTransferLeaderUpdateMetadata"
	stateTransferLeaderCloseOldLeader = "StateTransferLeaderCloseOldLeader"
	stateTransferLeaderOpenNewLeader  = "StateTransferLeaderOpenNewLeader"
	stateTransferLeaderFinish         = "StateTransferLeaderFinish"
)

var (
	transferLeaderEvents = fsm.Events{
		{Name: eventTransferLeaderUpdateMetadata, Src: []string{stateTransferLeaderBegin}, Dst: stateTransferLeaderUpdateMetadata},
		{Name: eventTransferLeaderCloseOldLeader, Src: []string{stateTransferLeaderUpdateMetadata}, Dst: stateTransferLeaderCloseOldLeader},
		{Name: eventTransferLeaderOpenNewLeader, Src: []string{stateTransferLeaderCloseOldLeader}, Dst: stateTransferLeaderOpenNewLeader},
		{Name: eventTransferLeaderFinish, Src: []string{stateTransferLeaderOpenNewLeader}, Dst: stateTransferLeaderFinish},
	}
	transferLeaderCallbacks = fsm.Callbacks{
		eventTransferLeaderUpdateMetadata: transferLeaderUpdateMetadataCallback,
		eventTransferLeaderCloseOldLeader: transferLeaderCloseOldLeaderCallback,
		eventTransferLeaderOpenNewLeader:  transferLeaderOpenNewShardCallback,
		eventTransferLeaderFinish:         transferLeaderFinishCallback,
	}
)

type TransferLeaderProcedure struct {
	id  uint64
	fsm *fsm.FSM

	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch
	storage  Storage

	shardID           storage.ShardID
	oldLeaderNodeName string
	newLeaderNodeName string

	// Protect the state.
	lock  sync.RWMutex
	state State
}

// TransferLeaderProcedurePersistRawData used for storage, procedure will be converted to persist raw data before saved in storage.
type TransferLeaderProcedurePersistRawData struct {
	ID                uint64
	FsmState          string
	ShardID           storage.ShardID
	OldLeaderNodeName string
	NewLeaderNodeName string
	State             State
}

// TransferLeaderCallbackRequest is fsm callbacks param
type TransferLeaderCallbackRequest struct {
	cluster  *cluster.Cluster
	ctx      context.Context
	dispatch eventdispatch.Dispatch

	shardID           storage.ShardID
	oldLeaderNodeName string
	newLeaderNodeName string
}

func NewTransferLeaderProcedure(dispatch eventdispatch.Dispatch, cluster *cluster.Cluster, storage Storage, shardID storage.ShardID, oldLeaderNodeName string, newLeaderNodeName string, id uint64) Procedure {
	transferLeaderOperationFsm := fsm.NewFSM(
		stateTransferLeaderBegin,
		transferLeaderEvents,
		transferLeaderCallbacks,
	)

	return &TransferLeaderProcedure{id: id, fsm: transferLeaderOperationFsm, dispatch: dispatch, cluster: cluster, storage: storage, shardID: shardID, oldLeaderNodeName: oldLeaderNodeName, newLeaderNodeName: newLeaderNodeName, state: StateInit}
}

func (p *TransferLeaderProcedure) ID() uint64 {
	return p.id
}

func (p *TransferLeaderProcedure) Typ() Typ {
	return TransferLeader
}

func (p *TransferLeaderProcedure) Start(ctx context.Context) error {
	p.updateStateWithLock(StateRunning)

	transferLeaderRequest := &TransferLeaderCallbackRequest{
		cluster:  p.cluster,
		ctx:      ctx,
		dispatch: p.dispatch,
	}

	for {
		switch p.fsm.Current() {
		case stateCreateTableBegin:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "transferLeader procedure persist failed")
			}
			if err := p.fsm.Event(eventTransferLeaderUpdateMetadata, transferLeaderRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessagef(err, "trasnferLeader procedure update metadata failed")
			}
		case stateTransferLeaderUpdateMetadata:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "transferLeader procedure persist failed")
			}
			if err := p.fsm.Event(eventTransferLeaderCloseOldLeader, transferLeaderRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessagef(err, "trasnferLeader procedure close old leader failed")
			}
		case stateTransferLeaderCloseOldLeader:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "transferLeader procedure persist failed")
			}
			if err := p.fsm.Event(eventTransferLeaderOpenNewLeader, transferLeaderRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessagef(err, "trasnferLeader procedure oepn new leader failed")
			}
		case stateTransferLeaderOpenNewLeader:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "transferLeader procedure persist failed")
			}
			if err := p.fsm.Event(eventTransferLeaderFinish, transferLeaderRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessagef(err, "trasnferLeader procedure finish failed")
			}
		case stateTransferLeaderFinish:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "transferLeader procedure persist failed")
			}
			p.updateStateWithLock(StateFinished)
			return nil
		}
	}
}

func (p *TransferLeaderProcedure) persist(ctx context.Context) error {
	meta, err := p.convertToMeta()
	if err != nil {
		return errors.WithMessagef(err, "convert")
	}
	err = p.storage.CreateOrUpdate(ctx, meta)
	if err != nil {
		return errors.WithMessagef(err, "createOrUpdate procedure storage failed")
	}
	return nil
}

func (p *TransferLeaderProcedure) Cancel(_ context.Context) error {
	p.updateStateWithLock(StateCancelled)
	return nil
}

func (p *TransferLeaderProcedure) State() State {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.state
}

func transferLeaderUpdateMetadataCallback(event *fsm.Event) {
	request := event.Args[0].(*TransferLeaderCallbackRequest)
	ctx := request.ctx

	if request.cluster.GetClusterState() != storage.ClusterStateStable {
		cancelEventWithLog(event, cluster.ErrClusterStateInvalid, "cluster state must be stable", zap.Int("currentState", int(request.cluster.GetClusterState())))
		return
	}

	shardNodes, err := request.cluster.GetShardNodesByShardID(request.shardID)
	if err != nil {
		cancelEventWithLog(event, err, "get shardNodes by shardID failed")
		return
	}

	found := false
	var leaderShardNode storage.ShardNode
	for _, shardNode := range shardNodes {
		if shardNode.ShardRole == storage.ShardRoleLeader {
			found = true
			leaderShardNode = shardNode
			leaderShardNode.NodeName = request.newLeaderNodeName
		}
	}
	if !found {
		cancelEventWithLog(event, ErrShardLeaderNotFound, "shard leader not found", zap.Uint32("shardID", uint32(request.shardID)))
		return
	}

	err = request.cluster.UpdateClusterView(ctx, storage.ClusterStateStable, []storage.ShardNode{leaderShardNode})
	if err != nil {
		cancelEventWithLog(event, storage.ErrUpdateClusterViewConflict, "update cluster view failed")
		return
	}
}

func transferLeaderCloseOldLeaderCallback(event *fsm.Event) {
	request := event.Args[0].(*TransferLeaderCallbackRequest)
	ctx := request.ctx

	closeShardRequest := eventdispatch.CloseShardRequest{
		ShardID: uint32(request.shardID),
	}
	if err := request.dispatch.CloseShard(ctx, request.oldLeaderNodeName, closeShardRequest); err != nil {
		cancelEventWithLog(event, err, "close shard failed", zap.Uint32("shardId", uint32(request.shardID)), zap.String("oldLeaderName", request.oldLeaderNodeName))
		return
	}
}

func transferLeaderOpenNewShardCallback(event *fsm.Event) {
	request := event.Args[0].(*TransferLeaderCallbackRequest)
	ctx := request.ctx

	// TODO: Shard version is only exists in ShardTables now, it is not sensible. Because shard version will be update when shard node mapping changed or shard table mapping changed, so ShardNodes also need shard version, consider refactor later.
	shardTablesMapping := request.cluster.GetTables([]storage.ShardID{request.shardID}, request.oldLeaderNodeName)
	shardTables, exists := shardTablesMapping[request.shardID]
	if !exists {
		cancelEventWithLog(event, cluster.ErrTableNotFound, "tables on shard not found")
		return
	}

	openShardRequest := eventdispatch.OpenShardRequest{
		Shard: cluster.ShardInfo{ID: request.shardID, Role: storage.ShardRoleLeader, Version: shardTables.Shard.Version + 1},
	}

	if err := request.dispatch.OpenShard(ctx, request.newLeaderNodeName, openShardRequest); err != nil {
		cancelEventWithLog(event, err, "open shard failed", zap.Uint32("shardId", uint32(request.shardID)), zap.String("newLeaderNode", request.newLeaderNodeName))
		return
	}
}

func transferLeaderFinishCallback(event *fsm.Event) {
	request := event.Args[0].(*TransferLeaderCallbackRequest)
	log.Info("transfer leader finish", zap.Uint32("shardID", uint32(request.shardID)), zap.String("oldLeaderNode", request.oldLeaderNodeName), zap.String("newLeaderNode", request.newLeaderNodeName))
}

func (p *TransferLeaderProcedure) updateStateWithLock(state State) {
	p.lock.Lock()
	p.state = state
	p.lock.Unlock()
}

func (p *TransferLeaderProcedure) convertToMeta() (Meta, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	rawData := TransferLeaderProcedurePersistRawData{
		ID:                p.id,
		FsmState:          p.fsm.Current(),
		ShardID:           p.shardID,
		OldLeaderNodeName: p.oldLeaderNodeName,
		NewLeaderNodeName: p.newLeaderNodeName,
		State:             p.state,
	}
	rawDataBytes, err := json.Marshal(rawData)
	if err != nil {
		return Meta{}, errors.WithMessage(err, "marshal raw data failed")
	}

	meta := Meta{
		ID:      p.id,
		Typ:     TransferLeader,
		State:   p.state,
		RawData: rawDataBytes,
	}

	return meta, nil
}

func ConvertToTransferLeaderProcedure(meta Meta, storage Storage, dispatch eventdispatch.Dispatch, cluster *cluster.Cluster) (*TransferLeaderProcedure, error) {
	if meta.Typ != TransferLeader {
		return nil, errors.WithMessagef(ErrProcedureTypeNotMatch, "expected type is:%d,real type is:%d", TransferLeader, meta.Typ)
	}

	var rawData TransferLeaderProcedurePersistRawData
	err := json.Unmarshal(meta.RawData, &rawData)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal raw data failed")
	}

	transferLeaderOperationFsm := fsm.NewFSM(
		rawData.FsmState,
		transferLeaderEvents,
		transferLeaderCallbacks,
	)

	return &TransferLeaderProcedure{
		id:                rawData.ID,
		fsm:               transferLeaderOperationFsm,
		cluster:           cluster,
		dispatch:          dispatch,
		storage:           storage,
		shardID:           rawData.ShardID,
		oldLeaderNodeName: rawData.OldLeaderNodeName,
		newLeaderNodeName: rawData.NewLeaderNodeName,
		state:             rawData.State,
	}, nil
}
