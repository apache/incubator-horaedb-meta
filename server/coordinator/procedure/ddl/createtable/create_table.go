/*
 * Copyright 2022 The CeresDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package createtable

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/assert"
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

const (
	eventCreateMetadata = "EventCreateMetadata"
	eventCreateOnShard  = "EventCreateOnShard"
	eventFinish         = "EventFinish"

	stateBegin          = "StateBegin"
	stateCreateMetadata = "StateCreateMetadata"
	stateCreateOnShard  = "StateCreateOnShard"
	stateFinish         = "StateFinish"
)

var (
	createTableEvents = fsm.Events{
		{Name: eventCreateMetadata, Src: []string{stateBegin}, Dst: stateCreateMetadata},
		{Name: eventCreateOnShard, Src: []string{stateCreateMetadata}, Dst: stateCreateOnShard},
		{Name: eventFinish, Src: []string{stateCreateOnShard}, Dst: stateFinish},
	}
	createTableCallbacks = fsm.Callbacks{
		eventCreateMetadata: createMetadataCallback,
		eventCreateOnShard:  createOnShard,
		eventFinish:         createFinish,
	}
)

func createMetadataCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	params := req.p.params

	createTableMetadataRequest := metadata.CreateTableMetadataRequest{
		SchemaName:    params.SourceReq.GetSchemaName(),
		TableName:     params.SourceReq.GetName(),
		PartitionInfo: storage.PartitionInfo{Info: params.SourceReq.PartitionTableInfo.GetPartitionInfo()},
	}
	_, err = params.ClusterMetadata.CreateTableMetadata(req.ctx, createTableMetadataRequest)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "create table metadata")
		return
	}

	log.Debug("create table metadata finish", zap.String("tableName", createTableMetadataRequest.TableName))
}

func createOnShard(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	params := req.p.params

	table, ok, err := params.ClusterMetadata.GetTable(params.SourceReq.GetSchemaName(), params.SourceReq.GetName())
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get table metadata failed", zap.String("schemaName", params.SourceReq.GetSchemaName()), zap.String("tableName", params.SourceReq.GetName()))
		return
	}
	if !ok {
		procedure.CancelEventWithLog(event, err, "table metadata not found", zap.String("schemaName", params.SourceReq.GetSchemaName()), zap.String("tableName", params.SourceReq.GetName()))
		return
	}

	shardVersionUpdate := metadata.ShardVersionUpdate{
		ShardID:       params.ShardID,
		LatestVersion: req.p.relatedVersionInfo.ShardWithVersion[params.ShardID],
	}

	createTableRequest := ddl.BuildCreateTableRequest(table, shardVersionUpdate, params.SourceReq)
	latestShardVersion, err := ddl.CreateTableOnShard(req.ctx, params.ClusterMetadata, params.Dispatch, params.ShardID, createTableRequest)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "dispatch create table on shard")
		return
	}

	log.Debug("dispatch createTableOnShard finish", zap.String("tableName", table.Name))

	shardVersionUpdate = metadata.ShardVersionUpdate{
		ShardID:       params.ShardID,
		LatestVersion: latestShardVersion,
	}

	err = params.ClusterMetadata.AddTableTopology(req.ctx, shardVersionUpdate, table)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "add table topology")
		return
	}

	req.createTableResult = &metadata.CreateTableResult{
		Table:              table,
		ShardVersionUpdate: shardVersionUpdate,
	}

	log.Debug("add table topology finish", zap.String("tableName", table.Name))
}

func createFinish(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}

	assert.Assert(req.createTableResult != nil)
	if err := req.p.params.OnSucceeded(*req.createTableResult); err != nil {
		log.Error("exec success callback failed")
	}
}

// callbackRequest is fsm callbacks param.
type callbackRequest struct {
	ctx context.Context
	p   *Procedure

	createTableResult *metadata.CreateTableResult

	persistData *persistData
}

type ProcedureParams struct {
	Dispatch        eventdispatch.Dispatch
	Storage         procedure.Storage
	ClusterMetadata *metadata.ClusterMetadata
	ClusterSnapshot metadata.Snapshot
	ID              uint64
	ShardID         storage.ShardID
	SourceReq       *metaservicepb.CreateTableRequest
	OnSucceeded     func(metadata.CreateTableResult) error
	OnFailed        func(error) error
}

func NewProcedure(params ProcedureParams) (procedure.Procedure, error) {
	fsm := fsm.NewFSM(
		stateBegin,
		createTableEvents,
		createTableCallbacks,
	)

	persistData, ok, err := loadPersistData(params.Storage, params.SourceReq.GetSchemaName(), params.SourceReq.GetName())
	if err != nil {
		return nil, err
	}
	// Reset procedure by persist data.
	if ok {
		log.Info("try to create table again", zap.String("schemaName", params.SourceReq.GetSchemaName()), zap.String("tableName", params.SourceReq.GetName()), zap.String("persistData", fmt.Sprintf("%v", persistData)))
		params.ShardID = persistData.ShardID
		fsm.SetState(persistData.FsmState)
	}

	// The ShardVersion associated with this ShardID is the latest version rather than the version returned by creating table on shard.
	relatedVersionInfo, err := buildRelatedVersionInfo(params)
	if err != nil {
		return nil, err
	}

	return &Procedure{
		fsm:                fsm,
		params:             params,
		relatedVersionInfo: relatedVersionInfo,
		state:              procedure.StateInit,
		storage:            params.Storage,
		lock:               sync.RWMutex{},
		persistData:        &persistData,
	}, nil
}

type Procedure struct {
	fsm                *fsm.FSM
	params             ProcedureParams
	relatedVersionInfo procedure.RelatedVersionInfo

	persistData *persistData
	storage     procedure.Storage
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

func (p *Procedure) Kind() procedure.Kind {
	return procedure.CreateTable
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateState(procedure.StateRunning)

	// Try to load persist data.
	req := &callbackRequest{
		ctx:               ctx,
		p:                 p,
		createTableResult: nil,
		persistData:       p.persistData,
	}

	for {
		switch p.fsm.Current() {
		case stateBegin:
			if err := p.persistProcedure(ctx); err != nil {
				return err
			}
			if err := p.fsm.Event(eventCreateMetadata, req); err != nil {
				_ = p.params.OnFailed(err)
				return err
			}
		case stateCreateMetadata:
			if err := p.persistProcedure(ctx); err != nil {
				return err
			}
			if err := p.fsm.Event(eventCreateOnShard, req); err != nil {
				_ = p.params.OnFailed(err)
				return err
			}
		case stateCreateOnShard:
			if err := p.persistProcedure(ctx); err != nil {
				return err
			}
			if err := p.fsm.Event(eventFinish, req); err != nil {
				_ = p.params.OnFailed(err)
				return err
			}
		case stateFinish:
			p.updateState(procedure.StateFinished)
			// TODO: Delete completed procedure.
			if err := p.persistProcedure(ctx); err != nil {
				return err
			}
			return nil
		}
	}
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

func (p *Procedure) persistProcedure(ctx context.Context) error {
	persistData := persistData{
		TableName:  p.params.SourceReq.GetName(),
		SchemaName: p.params.SourceReq.GetSchemaName(),
		FsmState:   p.fsm.Current(),
		ShardID:    p.params.ShardID,
	}
	meta, err := convertToMeta(persistData, p.ID(), p.Typ(), p.State())
	if err != nil {
		return err
	}
	if err := p.storage.CreateOrUpdate(ctx, meta); err != nil {
		return err
	}
	return nil
}

type persistData struct {
	// Target tableName and schemaName of this create table procedure.
	TableName  string
	SchemaName string

	// Target shardID of this create table procedure, shardID will generate and persist when this table create firstly.
	ShardID storage.ShardID

	// Current procedure fsm fsmState, procedure will retry from this state if create table failed and retry.
	FsmState string
}

func convertToMeta(data persistData, id uint64, typ procedure.Typ, state procedure.State) (procedure.Meta, error) {
	var emptyMeta procedure.Meta

	bytes, err := json.Marshal(data)
	if err != nil {
		return emptyMeta, err
	}

	meta := procedure.Meta{
		ID:      id,
		Typ:     typ,
		State:   state,
		RawData: bytes,
	}

	return meta, nil
}

func convertFromMeta(meta *procedure.Meta) (persistData, error) {
	var emptyPersistData persistData

	bytes := meta.RawData

	if err := json.Unmarshal(bytes, &emptyPersistData); err != nil {
		return emptyPersistData, err
	}

	return emptyPersistData, nil
}

func loadPersistData(storage procedure.Storage, schemaName string, tableName string) (persistData, bool, error) {
	var emptyPersistData persistData

	// Try to load persist data.
	metas, err := storage.List(context.Background(), 100)
	if err != nil {
		return emptyPersistData, false, err
	}

	// Find procedure by schema name and table name.
	// TODO: Optimize procedure query method and avoid using list to list all procedures.
	for _, meta := range metas {
		if meta.State != procedure.StateRunning {
			continue
		}
		p, err := convertFromMeta(meta)
		if err != nil {
			return emptyPersistData, false, err
		}
		if p.SchemaName == schemaName && p.TableName == tableName {
			return p, true, nil
		}
	}

	return emptyPersistData, false, nil
}
