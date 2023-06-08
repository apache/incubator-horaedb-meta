// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package createpartitiontable

import (
	"context"
	"encoding/json"
	"fmt"
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
// ┌────────┐     ┌──────────────────────────────┐     ┌────────────────────────────┐
// │ Begin  ├─────▶ CreatePartitionTableFinished ├─────▶  CreateDataTablesFinished  │
// └────────┘     └──────────────────────────────┘     └────────────────────────────┘
const (
	eventCreatePartitionTable = "EventCreatePartitionTable"
	eventCreateSubTables      = "EventCreateSubTables"

	stateBegin                        = "StateBegin"
	stateCreatePartitionTableFinished = "StateCreatePartitionTableFinished"
	stateCreateSubTablesFinished      = "StateCreateSubTablesFinished"

	leaveStateBegin                        = "leave_StateBegin"
	leaveStateCreatePartitionTableFinished = "leave_StateCreatePartitionTableFinished"
)

var (
	createPartitionTableEvents = fsm.Events{
		{Name: eventCreatePartitionTable, Src: []string{stateBegin}, Dst: stateCreatePartitionTableFinished},
		{Name: eventCreateSubTables, Src: []string{stateCreatePartitionTableFinished}, Dst: stateCreateSubTablesFinished},
	}
	createPartitionTableCallbacks = fsm.Callbacks{
		leaveStateBegin:                        createPartitionTableCallback,
		leaveStateCreatePartitionTableFinished: createDataTablesCallback,
	}
)

type Procedure struct {
	fsm                        *fsm.FSM
	params                     ProcedureParams
	relatedVersionInfo         procedure.RelatedVersionInfo
	createPartitionTableResult metadata.CreateTableMetadataResult

	lock  sync.RWMutex
	state procedure.State
}

type ProcedureParams struct {
	ID              uint64
	ClusterMetadata *metadata.ClusterMetadata
	ClusterSnapshot metadata.Snapshot
	Dispatch        eventdispatch.Dispatch
	Storage         procedure.Storage
	SourceReq       *metaservicepb.CreateTableRequest
	SubTablesShards []metadata.ShardNodeWithVersion
	OnSucceeded     func(metadata.CreateTableResult)
	OnFailed        func(error)
}

func NewProcedure(params ProcedureParams) (*Procedure, error) {
	relatedVersionInfo, err := buildRelatedVersionInfo(params)
	if err != nil {
		return nil, err
	}

	fsm := fsm.NewFSM(
		stateBegin,
		createPartitionTableEvents,
		createPartitionTableCallbacks,
	)

	return &Procedure{
		fsm:                fsm,
		relatedVersionInfo: relatedVersionInfo,
		params:             params,
		state:              procedure.StateInit,
	}, nil
}

func buildRelatedVersionInfo(params ProcedureParams) (procedure.RelatedVersionInfo, error) {
	shardWithVersion := make(map[storage.ShardID]uint64, len(params.SubTablesShards))
	for _, subTableShard := range params.SubTablesShards {
		shardView, exists := params.ClusterSnapshot.Topology.ShardViewsMapping[subTableShard.ShardInfo.ID]
		if !exists {
			return procedure.RelatedVersionInfo{}, errors.WithMessagef(metadata.ErrShardNotFound, "shard not found in topology, shardID:%d", subTableShard.ShardInfo.ID)
		}
		shardWithVersion[shardView.ShardID] = shardView.Version
	}

	return procedure.RelatedVersionInfo{
		ClusterID:        params.ClusterSnapshot.Topology.ClusterView.ClusterID,
		ShardWithVersion: shardWithVersion,
		ClusterVersion:   params.ClusterSnapshot.Topology.ClusterView.Version,
	}, nil
}

func (p *Procedure) ID() uint64 {
	return p.params.ID
}

func (p *Procedure) Typ() procedure.Typ {
	return procedure.CreatePartitionTable
}

func (p *Procedure) RelatedVersionInfo() procedure.RelatedVersionInfo {
	return p.relatedVersionInfo
}

func (p *Procedure) Priority() procedure.Priority {
	return procedure.PriorityLow
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateStateWithLock(procedure.StateRunning)

	createPartitionTableRequest := &callbackRequest{
		ctx: ctx,
		p:   p,
	}

	for {
		switch p.fsm.Current() {
		case stateBegin:
			if err := p.persist(ctx); err != nil {
				p.params.OnFailed(err)
				return errors.WithMessage(err, "persist create partition table procedure")
			}
			if err := p.fsm.Event(eventCreatePartitionTable, createPartitionTableRequest); err != nil {
				p.params.OnFailed(err)
				if _, ok := err.(fsm.CanceledError); ok {
					p.updateStateWithLock(procedure.StateCancelled)
					return errors.WithMessage(err, "create partition table canceled")
				}
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "create partition table")
			}
		case stateCreatePartitionTableFinished:
			if err := p.persist(ctx); err != nil {
				p.params.OnFailed(err)
				return errors.WithMessage(err, "persist create partition table procedure")
			}
			if err := p.fsm.Event(eventCreateSubTables, createPartitionTableRequest); err != nil {
				p.params.OnFailed(err)
				if _, ok := err.(fsm.CanceledError); ok {
					p.updateStateWithLock(procedure.StateCancelled)
					return errors.WithMessage(err, "create data tables canceled")
				}
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "create data tables")
			}
		case stateCreateSubTablesFinished:
			if err := p.persist(ctx); err != nil {
				p.params.OnFailed(err)
				return errors.WithMessage(err, "persist create partition table procedure")
			}
			p.updateStateWithLock(procedure.StateFinished)
			p.params.OnSucceeded(metadata.CreateTableResult{
				Table:              p.createPartitionTableResult.Table,
				ShardVersionUpdate: metadata.ShardVersionUpdate{},
			})
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

type callbackRequest struct {
	ctx context.Context
	p   *Procedure
}

// 1. Create partition table in target node.
func createPartitionTableCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	params := req.p.params

	createTableMetadaResult, err := params.ClusterMetadata.CreateTableMetadata(req.ctx, metadata.CreateTableMetadataRequest{
		SchemaName:    params.SourceReq.GetSchemaName(),
		TableName:     params.SourceReq.GetName(),
		PartitionInfo: storage.PartitionInfo{Info: params.SourceReq.PartitionTableInfo.GetPartitionInfo()},
	})
	if err != nil {
		procedure.CancelEventWithLog(event, err, "create table metadata")
		return
	}
	req.p.createPartitionTableResult = createTableMetadaResult
}

// 2. Create data tables in target nodes.
func createDataTablesCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	params := req.p.params
	if len(params.SubTablesShards) != len(params.SourceReq.GetPartitionTableInfo().SubTableNames) {
		panic(fmt.Sprintf("shards number must be equal to sub tables number, shardNumber:%d, subTableNumber:%d", len(params.SubTablesShards), len(params.SourceReq.GetPartitionTableInfo().SubTableNames)))
	}

	shardVersions := req.p.relatedVersionInfo.ShardWithVersion
	shardTableMetaDatas := make(map[storage.ShardID][]metadata.CreateTableMetadataRequest, 0)
	for i, subTableShard := range params.SubTablesShards {
		tableMetaData := metadata.CreateTableMetadataRequest{
			SchemaName:    params.SourceReq.GetSchemaName(),
			TableName:     params.SourceReq.GetPartitionTableInfo().SubTableNames[i],
			PartitionInfo: storage.PartitionInfo{},
		}
		shardTableMetaDatas[subTableShard.ShardInfo.ID] = append(shardTableMetaDatas[subTableShard.ShardInfo.ID], tableMetaData)
	}
	succeedCh := make(chan bool)
	errCh := make(chan error)
	for shardID, tableMetaDatas := range shardTableMetaDatas {
		shardVersion := shardVersions[shardID]
		go createDataTables(req, shardID, tableMetaDatas, shardVersion, succeedCh, errCh)
	}

	goRoutineNumber := len(shardTableMetaDatas)
	for {
		select {
		case err := <-errCh:
			procedure.CancelEventWithLog(event, err, "create data tables")
			return
		case <-succeedCh:
			goRoutineNumber--
			if goRoutineNumber == 0 {
				return
			}
		}
	}
}

func createDataTables(req *callbackRequest, shardID storage.ShardID, tableMetaDatas []metadata.CreateTableMetadataRequest, shardVersion uint64, succeedCh chan bool, errCh chan error) {
	params := req.p.params

	for _, tableMetaData := range tableMetaDatas {
		result, err := params.ClusterMetadata.CreateTableMetadata(req.ctx, tableMetaData)
		if err != nil {
			errCh <- errors.WithMessage(err, "create table metadata")
			return
		}

		shardVersionUpdate := metadata.ShardVersionUpdate{
			ShardID:     shardID,
			CurrVersion: shardVersion + 1,
			PrevVersion: shardVersion,
		}

		if err := ddl.CreateTableOnShard(req.ctx, params.ClusterMetadata, params.Dispatch, shardID, ddl.BuildCreateTableRequest(result.Table, shardVersionUpdate, params.SourceReq)); err != nil {
			errCh <- errors.WithMessage(err, "dispatch create table on shard")
			return
		}

		_, err = params.ClusterMetadata.AddTableTopology(req.ctx, shardID, result.Table)
		if err != nil {
			errCh <- errors.WithMessage(err, "create table metadata")
			return
		}
		shardVersion++
	}
	succeedCh <- true
}

func (p *Procedure) updateStateWithLock(state procedure.State) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.state = state
}

func (p *Procedure) persist(ctx context.Context) error {
	meta, err := p.convertToMeta()
	if err != nil {
		return errors.WithMessage(err, "convert to meta")
	}
	err = p.params.Storage.CreateOrUpdate(ctx, meta)
	if err != nil {
		return errors.WithMessage(err, "createOrUpdate procedure storage")
	}
	return nil
}

// TODO: Replace rawData with structure defined by proto.
type rawData struct {
	ID       uint64
	FsmState string
	State    procedure.State

	CreateTableResult    metadata.CreateTableResult
	PartitionTableShards []metadata.ShardNodeWithVersion
	SubTablesShards      []metadata.ShardNodeWithVersion
}

func (p *Procedure) convertToMeta() (procedure.Meta, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	rawData := rawData{
		ID:              p.params.ID,
		FsmState:        p.fsm.Current(),
		State:           p.state,
		SubTablesShards: p.params.SubTablesShards,
	}
	rawDataBytes, err := json.Marshal(rawData)
	if err != nil {
		return procedure.Meta{}, procedure.ErrEncodeRawData.WithCausef("marshal raw data, procedureID:%v, err:%v", p.params.ID, err)
	}

	meta := procedure.Meta{
		ID:    p.params.ID,
		Typ:   procedure.CreatePartitionTable,
		State: p.state,

		RawData: rawDataBytes,
	}

	return meta, nil
}
