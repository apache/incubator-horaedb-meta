// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package split

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

// Fsm state change: begin-> CreateNewShardView -> UpdateShardTables -> OpenNewShard -> Finish
// CreateNewShardView will create new shard metadata.
// UpdateShardTables will update shard tables mapping between the old and new shard.
// OpenNewShard will send open shard request to new shard leader.
const (
	eventCreateNewShardView = "EventCreateNewShardView"
	eventUpdateShardTables  = "EventUpdateShardTables"
	eventOpenNewShard       = "EventOpenNewShard"
	eventFinish             = "EventFinish"

	stateBegin              = "StateBegin"
	stateCreateNewShardView = "StateCreateNewShardView"
	stateUpdateShardTables  = "StateUpdateShardTables"
	stateOpenNewShard       = "StateOpenNewShard"
	stateFinish             = "StateFinish"
)

var (
	splitEvents = fsm.Events{
		{Name: eventCreateNewShardView, Src: []string{stateBegin}, Dst: stateCreateNewShardView},
		{Name: eventUpdateShardTables, Src: []string{stateCreateNewShardView}, Dst: stateUpdateShardTables},
		{Name: eventOpenNewShard, Src: []string{stateUpdateShardTables}, Dst: stateOpenNewShard},
		{Name: eventFinish, Src: []string{stateOpenNewShard}, Dst: stateFinish},
	}
	splitCallbacks = fsm.Callbacks{
		eventCreateNewShardView: createShardViewCallback,
		eventUpdateShardTables:  updateShardTablesCallback,
		eventOpenNewShard:       openShardCallback,
		eventFinish:             finishCallback,
	}
)

type Procedure struct {
	id uint64

	fsm *fsm.FSM

	dispatch        eventdispatch.Dispatch
	storage         procedure.Storage
	clusterMetadata *metadata.ClusterMetadata

	snapshot       metadata.Snapshot
	shardID        storage.ShardID
	newShardID     storage.ShardID
	tableNames     []string
	targetNodeName string
	schemaName     string

	// Protect the state.
	lock  sync.RWMutex
	state procedure.State
}

func NewProcedure(id uint64, dispatch eventdispatch.Dispatch, s procedure.Storage, clusterMetadata *metadata.ClusterMetadata, snapShot metadata.Snapshot, schemaName string, shardID storage.ShardID, newShardID storage.ShardID, tableNames []string, targetNodeName string) (procedure.Procedure, error) {
	// Validate cluster state.
	curState := snapShot.Topology.ClusterView.State
	if curState != storage.ClusterStateStable {
		log.Error("cluster state must be stable", zap.Error(metadata.ErrClusterStateInvalid))
		return nil, metadata.ErrClusterStateInvalid
	}

	found := false
	for _, shardView := range snapShot.Topology.ShardViews {
		if shardView.ShardID == shardID {
			found = true
			break
		}
	}
	if !found {
		log.Error("shard not found", zap.Uint64("shardID", uint64(shardID)), zap.Error(metadata.ErrShardNotFound))
		return nil, metadata.ErrShardNotFound
	}

	found = false
	for _, shardNode := range snapShot.Topology.ClusterView.ShardNodes {
		if shardNode.ShardRole == storage.ShardRoleLeader {
			found = true
		}
	}
	if !found {
		log.Error("shard leader not found", zap.Error(procedure.ErrShardLeaderNotFound))
		return nil, procedure.ErrShardLeaderNotFound
	}

	splitFsm := fsm.NewFSM(
		stateBegin,
		splitEvents,
		splitCallbacks,
	)

	return &Procedure{
		fsm:             splitFsm,
		id:              id,
		dispatch:        dispatch,
		clusterMetadata: clusterMetadata,
		snapshot:        snapShot,
		shardID:         shardID,
		newShardID:      newShardID,
		targetNodeName:  targetNodeName,
		tableNames:      tableNames,
		schemaName:      schemaName,
		storage:         s,
	}, nil
}

type callbackRequest struct {
	ctx context.Context

	dispatch        eventdispatch.Dispatch
	clusterMetadata *metadata.ClusterMetadata

	snapshot       metadata.Snapshot
	shardID        storage.ShardID
	newShardID     storage.ShardID
	schemaName     string
	tableNames     []string
	targetNodeName string
}

func (p *Procedure) ID() uint64 {
	return p.id
}

func (p *Procedure) Typ() procedure.Typ {
	return procedure.Split
}

func (p *Procedure) RelatedVersionInfo() procedure.RelatedVersionInfo {
	shardWithVersion := make(map[storage.ShardID]uint64, 0)
	for _, shardView := range p.snapshot.Topology.ShardViews {
		if shardView.ShardID == p.shardID {
			shardWithVersion[p.shardID] = shardView.Version
		}
	}
	shardWithVersion[p.newShardID] = 0
	return procedure.RelatedVersionInfo{
		ClusterID:        p.snapshot.Topology.ClusterView.ClusterID,
		ShardWithVersion: shardWithVersion,
		ClusterVersion:   p.snapshot.Topology.ClusterView.Version,
	}
}

func (p *Procedure) Priority() procedure.Priority {
	return procedure.PriorityHigh
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateStateWithLock(procedure.StateRunning)

	splitCallbackRequest := callbackRequest{
		ctx:             ctx,
		snapshot:        p.snapshot,
		dispatch:        p.dispatch,
		shardID:         p.shardID,
		newShardID:      p.newShardID,
		schemaName:      p.schemaName,
		tableNames:      p.tableNames,
		targetNodeName:  p.targetNodeName,
		clusterMetadata: p.clusterMetadata,
	}

	for {
		switch p.fsm.Current() {
		case stateBegin:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "split procedure persist")
			}
			if err := p.fsm.Event(eventCreateNewShardView, splitCallbackRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "split procedure create new shard view")
			}
		case stateCreateNewShardView:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "split procedure persist")
			}
			if err := p.fsm.Event(eventUpdateShardTables, splitCallbackRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "split procedure create new shard")
			}
		case stateUpdateShardTables:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "split procedure persist")
			}
			if err := p.fsm.Event(eventOpenNewShard, splitCallbackRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "split procedure create shard tables")
			}
		case stateOpenNewShard:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "split procedure persist")
			}
			if err := p.fsm.Event(eventFinish, splitCallbackRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "split procedure delete shard tables")
			}
		case stateFinish:
			// TODO: The state update sequence here is inconsistent with the previous one. Consider reconstructing the state update logic of the state machine.
			p.updateStateWithLock(procedure.StateFinished)
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "split procedure persist")
			}
			return nil
		}
	}
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

func (p *Procedure) updateStateWithLock(state procedure.State) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.state = state
}

func createShardViewCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	ctx := req.ctx

	if err := req.clusterMetadata.CreateShardViews(ctx, []metadata.CreateShardView{{
		ShardID: req.newShardID,
		Tables:  []storage.TableID{},
	}}); err != nil {
		procedure.CancelEventWithLog(event, err, "create shard views")
		return
	}
}

func updateShardTablesCallback(event *fsm.Event) {
	request, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}

	if err := request.clusterMetadata.MigrateTable(request.ctx, metadata.MigrateTableRequest{
		SchemaName: request.schemaName,
		TableNames: request.tableNames,
		OldShardID: request.shardID,
		NewShardID: request.newShardID,
	}); err != nil {
		procedure.CancelEventWithLog(event, err, "update shard tables")
		return
	}
}

func openShardCallback(event *fsm.Event) {
	request, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	ctx := request.ctx

	// Send open new shard request to CSE.
	if err := request.dispatch.OpenShard(ctx, request.targetNodeName, eventdispatch.OpenShardRequest{
		Shard: metadata.ShardInfo{
			ID:      request.newShardID,
			Role:    storage.ShardRoleLeader,
			Version: 0,
		},
	}); err != nil {
		procedure.CancelEventWithLog(event, err, "open shard failed")
		return
	}
}

func finishCallback(event *fsm.Event) {
	request, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	log.Info("split procedure finish", zap.Uint32("shardID", uint32(request.shardID)), zap.Uint32("newShardID", uint32(request.newShardID)))
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

type rawData struct {
	SchemaName     string
	TableNames     []string
	ShardID        uint32
	NewShardID     uint32
	TargetNodeName string
}

func (p *Procedure) convertToMeta() (procedure.Meta, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	rawData := rawData{
		SchemaName:     p.schemaName,
		TableNames:     p.tableNames,
		ShardID:        uint32(p.shardID),
		NewShardID:     uint32(p.newShardID),
		TargetNodeName: p.targetNodeName,
	}
	rawDataBytes, err := json.Marshal(rawData)
	if err != nil {
		return procedure.Meta{}, procedure.ErrEncodeRawData.WithCausef("marshal raw data, procedureID:%v, err:%v", p.shardID, err)
	}

	meta := procedure.Meta{
		ID:    p.id,
		Typ:   procedure.Split,
		State: p.state,

		RawData: rawDataBytes,
	}

	return meta, nil
}
