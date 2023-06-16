// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package createtable

import (
	"context"
	"sync"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/ddl"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
)

// fsm state change:
// ┌────────┐     ┌──────────────────────┐
// │ Begin  ├─────▶ CreateTableFinished  │
// └────────┘     └──────────────────────┘
const (
	eventCreateTable = "EventCreateTable"
	eventSucceeded   = "EventSucceeded "

	stateBegin       = "StateBegin"
	stateCreateTable = "StateCreateTable"
	stateFinished    = "StateFinished"
)

var (
	createTableEvents = fsm.Events{
		{Name: eventCreateTable, Src: []string{stateBegin}, Dst: stateCreateTable},
		{Name: eventSucceeded, Src: []string{stateCreateTable}, Dst: stateFinished},
	}
	createTableCallbacks = fsm.Callbacks{
		eventCreateTable: createTableCallback,
		eventSucceeded:   succeededCallback,
	}
)

func createTableCallback(ctx context.Context, event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, procedure.FSMError{Err: err}, "get request from event")
		return
	}
	params := req.p.params

	createTableMetadataRequest := metadata.CreateTableMetadataRequest{
		SchemaName:    params.SourceReq.GetSchemaName(),
		TableName:     params.SourceReq.GetName(),
		PartitionInfo: storage.PartitionInfo{Info: params.SourceReq.PartitionTableInfo.GetPartitionInfo()},
	}
	result, err := params.ClusterMetadata.CreateTableMetadata(req.ctx, createTableMetadataRequest)
	if err != nil {
		procedure.CancelEventWithLog(event, procedure.FSMError{Err: err}, "create table metadata")
		return
	}

	shardVersionUpdate := metadata.ShardVersionUpdate{
		ShardID:     params.ShardID,
		CurrVersion: req.p.relatedVersionInfo.ShardWithVersion[params.ShardID] + 1,
		PrevVersion: req.p.relatedVersionInfo.ShardWithVersion[params.ShardID],
	}

	createTableRequest := ddl.BuildCreateTableRequest(result.Table, shardVersionUpdate, params.SourceReq)
	if err = ddl.CreateTableOnShard(req.ctx, params.ClusterMetadata, params.Dispatch, params.ShardID, createTableRequest); err != nil {
		procedure.CancelEventWithLog(event, procedure.FSMError{Err: err}, "dispatch create table on shard")
		return
	}

	createTableResult, err := params.ClusterMetadata.AddTableTopology(req.ctx, params.ShardID, result.Table)
	if err != nil {
		procedure.CancelEventWithLog(event, procedure.FSMError{Err: err}, "create table metadata")
		return
	}

	req.createTableResult = createTableResult

	err = req.p.fsm.Event(ctx, eventSucceeded, req)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "create table metadata")
	}
}

func succeededCallback(_ context.Context, event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, procedure.FSMError{Err: err}, "get request from event")
		return
	}

	req.p.params.OnSucceeded(req.createTableResult)
}

// callbackRequest is fsm callbacks param.
type callbackRequest struct {
	ctx               context.Context
	p                 *Procedure
	createTableResult metadata.CreateTableResult
}

type ProcedureParams struct {
	Dispatch        eventdispatch.Dispatch
	ClusterMetadata *metadata.ClusterMetadata
	ClusterSnapshot metadata.Snapshot
	ID              uint64
	ShardID         storage.ShardID
	SourceReq       *metaservicepb.CreateTableRequest
	OnSucceeded     func(metadata.CreateTableResult)
	OnFailed        func(error)
}

func NewProcedure(params ProcedureParams) (procedure.Procedure, error) {
	fsm := fsm.NewFSM(
		stateBegin,
		createTableEvents,
		createTableCallbacks,
	)

	relatedVersionInfo, err := buildRelatedVersionInfo(params)
	if err != nil {
		return nil, err
	}

	return &Procedure{
		fsm:                fsm,
		params:             params,
		relatedVersionInfo: relatedVersionInfo,
		state:              procedure.StateInit,
	}, nil
}

type Procedure struct {
	fsm                *fsm.FSM
	params             ProcedureParams
	relatedVersionInfo procedure.RelatedVersionInfo
	// Protect the state.
	lock  sync.RWMutex
	state procedure.State
}

func (p *Procedure) RelatedVersionInfo() procedure.RelatedVersionInfo {
	return p.relatedVersionInfo
}

func buildRelatedVersionInfo(params ProcedureParams) (procedure.RelatedVersionInfo, error) {
	shardWithVersion := make(map[storage.ShardID]uint64, 1)
	shardView, exists := params.ClusterSnapshot.Topology.ShardViewsMapping[params.ShardID]
	if !exists {
		return procedure.RelatedVersionInfo{}, errors.WithMessagef(metadata.ErrShardNotFound, "shard not found in topology, shardID:%d", params.ShardID)
	}
	shardWithVersion[params.ShardID] = shardView.Version
	return procedure.RelatedVersionInfo{
		ClusterID:        params.ClusterSnapshot.Topology.ClusterView.ClusterID,
		ShardWithVersion: shardWithVersion,
		ClusterVersion:   params.ClusterSnapshot.Topology.ClusterView.Version,
	}, nil
}

func (p *Procedure) Priority() procedure.Priority {
	return procedure.PriorityLow
}

func (p *Procedure) ID() uint64 {
	return p.params.ID
}

func (p *Procedure) Typ() procedure.Typ {
	return procedure.CreateTable
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateState(procedure.StateRunning)

	req := &callbackRequest{
		ctx: ctx,
		p:   p,
	}

	retrySize := 3
	var err error
	for i := 0; i < retrySize; i++ {
		err = p.fsm.Event(ctx, p.fsm.AvailableTransitions()[0], req)
		if err == nil {
			p.updateState(procedure.StateFinished)
			return nil
		}
		if _, ok := err.(procedure.FSMError); !ok {
			break
		}
	}

	p.params.OnFailed(err)
	p.updateState(procedure.StateFailed)
	return errors.WithMessage(err, "create table")
}

func (p *Procedure) Cancel(_ context.Context) error {
	p.updateState(procedure.StateCancelled)
	return nil
}

func (p *Procedure) State() procedure.State {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.state
}

func (p *Procedure) updateState(state procedure.State) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.state = state
}
