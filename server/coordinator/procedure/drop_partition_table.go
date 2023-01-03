// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// fsm state change:
// ┌────────┐     ┌──────────────────────┐     ┌────────────────────┐     ┌───────────┐
// │ Begin  ├─────▶     DropDataTable    ├─────▶ DropPartitionTable ├─────▶  Finish   │
// └────────┘     └──────────────────────┘     └────────────────────┘     └───────────┘
const (
	eventDropDataTable            = "EventDropDataTable"
	eventDropPartitionTable       = "EventDropPartitionTable"
	eventDropPartitionTableFinish = "EventSuccess"

	stateDropPartitionTableBegin  = "StateBegin"
	stateDropDataTable            = "StateDropDataTable"
	stateDropPartitionTable       = "StateDropPartitionTable"
	stateDropPartitionTableFinish = "StateFinish"
)

var (
	createDropPartitionTableEvents = fsm.Events{
		{Name: eventDropDataTable, Src: []string{stateDropPartitionTableBegin}, Dst: stateDropDataTable},
		{Name: eventDropPartitionTable, Src: []string{stateDropDataTable}, Dst: stateDropPartitionTable},
		{Name: eventDropPartitionTableFinish, Src: []string{stateDropPartitionTable}, Dst: stateDropPartitionTableFinish},
	}
	createDropPartitionTableCallbacks = fsm.Callbacks{
		eventDropDataTable:            dropDataTablesCallback,
		eventDropPartitionTable:       dropPartitionTableCallback,
		eventDropPartitionTableFinish: finishDropPartitionTableCallback,
	}
)

type DropPartitionTableProcedure struct {
	id       uint64
	fsm      *fsm.FSM
	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch
	storage  Storage

	req *metaservicepb.DropTableRequest

	onSucceeded func(cluster.TableInfo) error
	onFailed    func(error) error

	lock  sync.RWMutex
	state State
}

type DropPartitionTableProcedureRequest struct {
	ID          uint64
	Cluster     *cluster.Cluster
	Dispatch    eventdispatch.Dispatch
	Storage     Storage
	Req         *metaservicepb.DropTableRequest
	OnSucceeded func(result cluster.TableInfo) error
	OnFailed    func(error) error
}

func NewDropPartitionTableProcedure(request DropPartitionTableProcedureRequest) *DropPartitionTableProcedure {
	fsm := fsm.NewFSM(
		stateDropPartitionTableBegin,
		createDropPartitionTableEvents,
		createDropPartitionTableCallbacks,
	)
	return &DropPartitionTableProcedure{
		id:          request.ID,
		fsm:         fsm,
		cluster:     request.Cluster,
		dispatch:    request.Dispatch,
		storage:     request.Storage,
		req:         request.Req,
		onSucceeded: request.OnSucceeded,
		onFailed:    request.OnFailed,
	}
}

func (p *DropPartitionTableProcedure) ID() uint64 {
	return p.id
}

func (p *DropPartitionTableProcedure) Typ() Typ {
	return DropPartitionTable
}

func (p *DropPartitionTableProcedure) Start(ctx context.Context) error {
	p.updateStateWithLock(StateRunning)

	dropPartitionTableRequest := &dropPartitionTableCallbackRequest{
		ctx:         ctx,
		cluster:     p.cluster,
		dispatch:    p.dispatch,
		sourceReq:   p.req,
		onSucceeded: p.onSucceeded,
		onFailed:    p.onFailed,
	}

	for {
		switch p.fsm.Current() {
		case stateDropPartitionTableBegin:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "drop partition table procedure persist")
			}
			if err := p.fsm.Event(eventDropDataTable, dropPartitionTableRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessagef(err, "drop partition table procedure create new shard view")
			}
		case stateDropDataTable:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "drop partition table procedure persist")
			}
			if err := p.fsm.Event(eventDropPartitionTable, dropPartitionTableRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessagef(err, "drop partition table procedure drop partition table")
			}
		case stateDropPartitionTable:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "drop partition table procedure persist")
			}
			if err := p.fsm.Event(eventDropPartitionTableFinish, dropPartitionTableRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessagef(err, "drop partition table procedure open partition tables")
			}
			p.updateStateWithLock(StateFinished)
		case stateDropPartitionTableFinish:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "drop partition table procedure persist")
			}
			return nil
		}
	}
}

func (p *DropPartitionTableProcedure) Cancel(_ context.Context) error {
	p.updateStateWithLock(StateCancelled)
	return nil
}

func (p *DropPartitionTableProcedure) State() State {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.state
}

type dropPartitionTableCallbackRequest struct {
	ctx      context.Context
	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch

	sourceReq *metaservicepb.DropTableRequest

	onSucceeded func(info cluster.TableInfo) error
	onFailed    func(error) error

	ret cluster.TableInfo
}

// 1. Drop data tables in target nodes.
func dropDataTablesCallback(event *fsm.Event) {
	req, err := getRequestFromEvent[*dropPartitionTableCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}

	for _, tableName := range req.sourceReq.PartitionTableInfo.SubTableNames {
		err := dropTable(event, tableName)
		if err != nil {
			cancelEventWithLog(event, err, fmt.Sprintf("drop table, table:%s", tableName))
		}
	}
}

// 2. Drop partition table in target node.
func dropPartitionTableCallback(event *fsm.Event) {
	req, err := getRequestFromEvent[*dropPartitionTableCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}

	err = dropTable(event, req.sourceReq.Name)
	if err != nil {
		cancelEventWithLog(event, err, fmt.Sprintf("drop table, table:%s", req.sourceReq.Name))
	}
}

func finishDropPartitionTableCallback(event *fsm.Event) {
	req, err := getRequestFromEvent[*dropPartitionTableCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}
	log.Info("drop partition table finish")

	if err := req.onSucceeded(req.ret); err != nil {
		cancelEventWithLog(event, err, "drop partition table on succeeded")
		return
	}
}

func dropTable(event *fsm.Event, tableName string) error {
	request, err := getRequestFromEvent[*dropPartitionTableCallbackRequest](event)
	if err != nil {
		return errors.WithMessage(err, "get request from event")
	}
	table, exists, err := request.cluster.GetTable(request.sourceReq.GetSchemaName(), tableName)
	if err != nil {
		return errors.WithMessage(err, "cluster get table")
	}
	if !exists {
		log.Warn("drop non-existing table", zap.String("schema", request.sourceReq.GetSchemaName()), zap.String("table", tableName))
		return nil
	}

	shardNodesResult, err := request.cluster.GetShardNodeByTableIDs([]storage.TableID{table.ID})
	if err != nil {
		return errors.WithMessage(err, "cluster get shard by table id")
	}

	result, err := request.cluster.DropTable(request.ctx, request.sourceReq.GetSchemaName(), tableName)
	if err != nil {
		return errors.WithMessage(err, "cluster drop table")
	}

	shardNodes, ok := shardNodesResult.ShardNodes[table.ID]
	if !ok {
		return errors.WithMessagef(err, "cluster get shard by table id, table:%v", table)
	}

	// TODO: consider followers
	leader := storage.ShardNode{}
	found := false
	for _, shardNode := range shardNodes {
		if shardNode.ShardRole == storage.ShardRoleLeader {
			found = true
			leader = shardNode
			break
		}
	}

	if !found {
		return errors.WithMessage(err, "can't find leader")
	}

	tableInfo := cluster.TableInfo{
		ID:         table.ID,
		Name:       table.Name,
		SchemaID:   table.SchemaID,
		SchemaName: request.sourceReq.GetSchemaName(),
	}
	err = request.dispatch.DropTableOnShard(request.ctx, leader.NodeName, eventdispatch.DropTableOnShardRequest{
		UpdateShardInfo: eventdispatch.UpdateShardInfo{
			CurrShardInfo: cluster.ShardInfo{
				ID:      result.ShardVersionUpdate.ShardID,
				Role:    storage.ShardRoleLeader,
				Version: result.ShardVersionUpdate.CurrVersion,
			},
			PrevVersion: result.ShardVersionUpdate.PrevVersion,
		},
		TableInfo: tableInfo,
	})
	if err != nil {
		return errors.WithMessage(err, "dispatch drop table on shard")
	}
	return nil
}

func (p *DropPartitionTableProcedure) updateStateWithLock(state State) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.state = state
}

func (p *DropPartitionTableProcedure) persist(ctx context.Context) error {
	meta, err := p.convertToMeta()
	if err != nil {
		return errors.WithMessage(err, "convert to meta")
	}
	err = p.storage.CreateOrUpdate(ctx, meta)
	if err != nil {
		return errors.WithMessage(err, "createOrUpdate procedure Storage")
	}
	return nil
}

type dropPartitionTableRawData struct {
	ID       uint64
	FsmState string
	State    State

	DropTableResult cluster.DropTableResult
}

func (p *DropPartitionTableProcedure) convertToMeta() (Meta, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	rawData := dropPartitionTableRawData{
		ID:       p.id,
		FsmState: p.fsm.Current(),
		State:    p.state,
	}
	rawDataBytes, err := json.Marshal(rawData)
	if err != nil {
		return Meta{}, ErrEncodeRawData.WithCausef("marshal raw data, procedureID:%v, err:%v", p.id, err)
	}

	meta := Meta{
		ID:    p.id,
		Typ:   DropPartitionTable,
		State: p.state,

		RawData: rawDataBytes,
	}

	return meta, nil
}
