// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package createpartitiontable

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/ddl"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// fsm state change:
// ┌────────┐     ┌──────────────────────┐     ┌────────────────────┐     ┌───────────┐
// │ Begin  ├─────▶ CreatePartitionTable ├─────▶  CreateDataTables  ├──────▶  Finish  │
// └────────┘     └──────────────────────┘     └────────────────────┘     └───────────┘
const (
	eventCreatePartitionTable = "EventCreatePartitionTable"
	eventCreateSubTables      = "EventCreateSubTables"
	eventFinish               = "EventFinish"

	stateBegin                = "StateBegin"
	stateCreatePartitionTable = "StateCreatePartitionTable"
	stateCreateSubTables      = "StateCreateSubTables"
	stateFinish               = "StateFinish"
)

var (
	createPartitionTableEvents = fsm.Events{
		{Name: eventCreatePartitionTable, Src: []string{stateBegin}, Dst: stateCreatePartitionTable},
		{Name: eventCreateSubTables, Src: []string{stateCreatePartitionTable}, Dst: stateCreateSubTables},
		{Name: eventFinish, Src: []string{stateCreateSubTables}, Dst: stateFinish},
	}
	createPartitionTableCallbacks = fsm.Callbacks{
		eventCreatePartitionTable: createPartitionTableCallback,
		eventCreateSubTables:      createDataTablesCallback,
		eventFinish:               finishCallback,
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
	OnSucceeded     func(metadata.CreateTableResult) error
	OnFailed        func(error) error
}

func NewProcedure(params ProcedureParams) *Procedure {
	relatedVersionInfo := buildRelatedVersionInfo(params)

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
	}
}

func buildRelatedVersionInfo(params ProcedureParams) procedure.RelatedVersionInfo {
	shardWithVersion := make(map[storage.ShardID]uint64, len(params.SubTablesShards))
	shardIDs := make([]storage.ShardID, 0, len(params.SubTablesShards))
	for _, shardView := range params.SubTablesShards {
		shardIDs = append(shardIDs, shardView.ShardInfo.ID)
	}
	for _, shardID := range shardIDs {
		for _, shardView := range params.ClusterSnapshot.Topology.ShardViews {
			if shardView.ShardID == shardID {
				shardWithVersion[shardID] = shardView.Version
				continue
			}
		}
	}
	return procedure.RelatedVersionInfo{
		ClusterID:        params.ClusterSnapshot.Topology.ClusterView.ClusterID,
		ShardWithVersion: shardWithVersion,
		ClusterVersion:   params.ClusterSnapshot.Topology.ClusterView.Version,
	}
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
				return errors.WithMessage(err, "persist create partition table procedure")
			}
			if err := p.fsm.Event(eventCreatePartitionTable, createPartitionTableRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "create partition table")
			}
		case stateCreatePartitionTable:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "persist create partition table procedure")
			}
			if err := p.fsm.Event(eventCreateSubTables, createPartitionTableRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "create data tables")
			}
		case stateCreateSubTables:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "persist create partition table procedure")
			}
			if err := p.fsm.Event(eventFinish, createPartitionTableRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				return errors.WithMessage(err, "update table shard metadata")
			}
		case stateFinish:
			// TODO: The state update sequence here is inconsistent with the previous one. Consider reconstructing the state update logic of the state machine.
			p.updateStateWithLock(procedure.StateFinished)
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "create partition table procedure persist")
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

	for i, subTableShard := range params.SubTablesShards {
		createTableResult, err := ddl.CreateTableMetadata(req.ctx, params.ClusterMetadata, params.SourceReq.GetSchemaName(), params.SourceReq.GetPartitionTableInfo().SubTableNames[i], subTableShard.ShardInfo.ID, nil)
		if err != nil {
			procedure.CancelEventWithLog(event, err, "create table metadata")
			return
		}

		if err = ddl.CreateTableOnShard(req.ctx, params.ClusterMetadata, params.Dispatch, subTableShard.ShardInfo.ID, ddl.BuildCreateTableRequest(createTableResult, params.SourceReq, nil)); err != nil {
			procedure.CancelEventWithLog(event, err, "dispatch create table on shard")
			return
		}
	}
}

func finishCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	log.Info("create partition table finish", zap.String("tableName", req.p.params.SourceReq.GetName()))

	if err := req.p.params.OnSucceeded(metadata.CreateTableResult{
		Table:              req.p.createPartitionTableResult.Table,
		ShardVersionUpdate: metadata.ShardVersionUpdate{},
	}); err != nil {
		procedure.CancelEventWithLog(event, err, "create partition table on succeeded")
		return
	}
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
