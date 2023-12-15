package transfertable

import (
	"context"
	"fmt"
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

// Fsm state change: Begin -> CloseTable -> OpenTable -> Finish.
// CloseTable will send close table on shard request to old shard.
// OpenTable will send open table on shard request to new shard.
const (
	eventCloseTable = "EventCloseTable"
	eventOpenTable  = "EventOpenTable"
	eventFinish     = "EventFinish"

	stateBegin      = "StateBegin"
	stateCloseTable = "StateCloseTable"
	stateOpenTable  = "StateOpenTable"
	stateFinish     = "StateFinish"
)

var (
	transferTableEvents = fsm.Events{
		{Name: eventCloseTable, Src: []string{stateBegin}, Dst: stateCloseTable},
		{Name: eventOpenTable, Src: []string{stateBegin, stateCloseTable}, Dst: stateOpenTable},
		{Name: eventFinish, Src: []string{stateOpenTable}, Dst: stateFinish},
	}
	transferTableCallbacks = fsm.Callbacks{
		eventCloseTable: closeTableCallback,
		eventOpenTable:  openTableCallback,
		eventFinish:     finishCallback,
	}
)

type Procedure struct {
	fsm    *fsm.FSM
	params ProcedureParams

	tableInfo               metadata.TableInfo
	hasSrcShardNode         bool
	srcShardNodeWithVersion metadata.ShardNodeWithVersion
	relatedVersionInfo      procedure.RelatedVersionInfo

	// Protect the state.
	// FIXME: the procedure should be executed sequentially, so any need to use a lock to protect it?
	lock  sync.RWMutex
	state procedure.State
}

// callbackRequest is fsm callbacks param.
type callbackRequest struct {
	ctx context.Context
	p   *Procedure
}

type ProcedureParams struct {
	Dispatch        eventdispatch.Dispatch
	ClusterMetadata *metadata.ClusterMetadata
	ID              uint64
	SchemaName      string
	TableName       string
	DestShardID     storage.ShardID
	OnSucceeded     func() error
	OnFailed        func(error) error
}

func NewProcedure(params ProcedureParams) (procedure.Procedure, error) {
	clusterMetadata := params.ClusterMetadata
	schemaName := params.SchemaName
	tableName := params.TableName
	routeTable, err := clusterMetadata.RouteTables(context.Background(), schemaName, []string{tableName})
	if err != nil {
		return nil, metadata.ErrTransferTable.WithCause(err)
	}

	entry, ok := routeTable.RouteEntries[tableName]
	if !ok {
		return nil, metadata.ErrTransferTable.WithCausef("table not found")
	}

	shardWithVersion := map[storage.ShardID]uint64{}
	hasSrcShardNode := false
	srcShardNodeWithVersion := metadata.ShardNodeWithVersion{}
	if len(entry.NodeShards) != 0 {
		hasSrcShardNode = true
		srcShardNodeWithVersion = entry.NodeShards[0]
		shardWithVersion[srcShardNodeWithVersion.ShardID()] = srcShardNodeWithVersion.ShardInfo.Version
	}

	tableInfo, ok := routeTable.GetTableInfo(tableName)
	if !ok {
		return nil, metadata.ErrTransferTable.WithCausef("table not found")
	}

	relatedVersionInfo := procedure.RelatedVersionInfo{
		ClusterID:        clusterMetadata.GetClusterID(),
		ShardWithVersion: shardWithVersion,
		ClusterVersion:   clusterMetadata.GetClusterViewVersion(),
	}

	transferTableOperationFsm := fsm.NewFSM(
		stateBegin,
		transferTableEvents,
		transferTableCallbacks,
	)

	return &Procedure{
		fsm:                     transferTableOperationFsm,
		params:                  params,
		tableInfo:               tableInfo,
		hasSrcShardNode:         hasSrcShardNode,
		srcShardNodeWithVersion: srcShardNodeWithVersion,
		relatedVersionInfo:      relatedVersionInfo,
		lock:                    sync.RWMutex{},
		state:                   procedure.StateInit,
	}, nil
}

func (p *Procedure) ID() uint64 {
	return p.params.ID
}

func (p *Procedure) Typ() procedure.Typ {
	return procedure.TransferTable
}

func (p *Procedure) Priority() procedure.Priority {
	return procedure.PriorityLow
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateStateWithLock(procedure.StateRunning)

	transferTableRequest := callbackRequest{
		ctx: ctx,
		p:   p,
	}

	for {
		switch p.fsm.Current() {
		case stateBegin:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "transferTable procedure persist")
			}

			if p.hasSrcShardNode {
				if err := p.fsm.Event(eventCloseTable, transferTableRequest); err != nil {
					p.updateStateWithLock(procedure.StateFailed)
					_ = p.params.OnFailed(err)
					return errors.WithMessage(err, "transferTable procedure close table on shard")
				}
			} else {
				if err := p.fsm.Event(eventOpenTable, transferTableRequest); err != nil {
					p.updateStateWithLock(procedure.StateFailed)
					_ = p.params.OnFailed(err)
					return errors.WithMessage(err, "transferTable procedure open table on shard")
				}
			}

		case stateCloseTable:
			if err := p.persist(ctx); err != nil {
				_ = p.params.OnFailed(err)
				return errors.WithMessage(err, "transferTable procedure persist")
			}
			if err := p.fsm.Event(eventOpenTable, transferTableRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				_ = p.params.OnFailed(err)
				return errors.WithMessage(err, "transferTable procedure open table on shard")
			}
		case stateOpenTable:
			if err := p.persist(ctx); err != nil {
				_ = p.params.OnFailed(err)
				return errors.WithMessage(err, "transferTable procedure persist")
			}
			if err := p.fsm.Event(eventFinish, transferTableRequest); err != nil {
				p.updateStateWithLock(procedure.StateFailed)
				_ = p.params.OnFailed(err)
				return errors.WithMessage(err, "transferTable procedure finish")
			}
		case stateFinish:
			// TODO: The state update sequence here is inconsistent with the previous one. Consider reconstructing the state update logic of the state machine.
			p.updateStateWithLock(procedure.StateFinished)
			if err := p.persist(ctx); err != nil {
				_ = p.params.OnFailed(err)
				return errors.WithMessage(err, "transferTable procedure persist")
			}
			return nil
		}
	}
}

func (p *Procedure) RelatedVersionInfo() procedure.RelatedVersionInfo {
	return p.relatedVersionInfo
}

func (p *Procedure) persist(_ctx context.Context) error {
	// TransferTable procedure is not persistent.
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

func closeTableCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}

	clusterMetadata := req.p.params.ClusterMetadata
	schemaName := req.p.params.SchemaName
	tableName := req.p.params.TableName
	srcShardNodeWithVersion := req.p.srcShardNodeWithVersion
	tableInfo := req.p.tableInfo

	closeTableOnShardRequest := eventdispatch.CloseTableOnShardRequest{
		TableInfo: tableInfo,
		UpdateShardInfo: eventdispatch.UpdateShardInfo{
			CurrShardInfo: srcShardNodeWithVersion.ShardInfo,
		},
	}

	ctx := req.ctx
	nodeAddr := srcShardNodeWithVersion.NodeName()
	// Currently, we ignore the response of CloseTableOnShard.
	_, err = req.p.params.Dispatch.CloseTableOnShard(ctx, nodeAddr, closeTableOnShardRequest)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "close table on shard", zap.Uint32("srcShardID", uint32(srcShardNodeWithVersion.ShardID())), zap.String("nodeAddr", nodeAddr))
		return
	}

	err = clusterMetadata.RemoveTableTopology(ctx, srcShardNodeWithVersion.ShardID(), srcShardNodeWithVersion.Version(), tableInfo.ID)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "remove table from topology", zap.Uint32("srcShardID", uint32(srcShardNodeWithVersion.ShardID())), zap.String("nodeAddr", nodeAddr))
		return
	}

	log.Info("close table on shard finish", zap.Uint64("procedureID", req.p.ID()), zap.String("schemaName", schemaName), zap.String("tableName", tableName), zap.Uint64("srcShardID", uint64(srcShardNodeWithVersion.ShardID())))
}

func openTableCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	ctx := req.ctx
	shardID := req.p.params.DestShardID
	clusterMetadata := req.p.params.ClusterMetadata
	tableInfo := req.p.tableInfo
	nodes, err := clusterMetadata.GetShardNodesByShardID(shardID)
	if err != nil || len(nodes) == 0 {
		procedure.CancelEventWithLog(event, err, fmt.Sprintf("get shard nodes by shardID:%d", shardID))
		return
	}
	nodeAddr := nodes[0].NodeName
	openShardRequest := eventdispatch.OpenTableOnShardRequest{
		UpdateShardInfo: eventdispatch.UpdateShardInfo{CurrShardInfo: metadata.ShardInfo{ID: shardID}},
		TableInfo:       req.p.tableInfo,
	}

	resp, err := req.p.params.Dispatch.OpenTableOnShard(ctx, nodeAddr, openShardRequest)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "open shard", zap.Uint32("shardID", uint32(shardID)), zap.String("nodeAddr", nodeAddr))
		return
	}

	if err != nil {
		procedure.CancelEventWithLog(event, err, "get shard version from topology")
		return
	}
	shardVersionUpdate := metadata.ShardVersionUpdate{ShardID: shardID, LatestVersion: resp.Version}
	err = clusterMetadata.AddTableTopology(ctx, shardVersionUpdate, storage.Table{
		ID:            tableInfo.ID,
		Name:          tableInfo.Name,
		SchemaID:      tableInfo.SchemaID,
		PartitionInfo: tableInfo.PartitionInfo,
		CreatedAt:     tableInfo.CreatedAt,
	})
	if err != nil {
		procedure.CancelEventWithLog(event, err, "add table topology")
		return
	}

	log.Info("try to open table on shard", zap.Uint64("procedureID", req.p.ID()), zap.String("schemaName", req.p.params.SchemaName), zap.String("tableName", req.p.params.TableName), zap.Uint64("shardID", uint64(req.p.params.DestShardID)))

	log.Info("open shard finish", zap.Uint64("procedureID", req.p.ID()), zap.Uint64("shardID", uint64(shardID)), zap.String("nodeAddr", nodeAddr))
}

func finishCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}

	schemaName := req.p.params.SchemaName
	tableName := req.p.params.TableName
	destShardID := req.p.params.DestShardID

	log.Info("transfer table finish", zap.Uint64("procedureID", req.p.ID()), zap.String("schemaName", schemaName), zap.String("tableName", tableName), zap.Uint64("destShardID", uint64(destShardID)))

	if err := req.p.params.OnSucceeded(); err != nil {
		procedure.CancelEventWithLog(event, err, "transfer table succeeded")
		return
	}
}

func (p *Procedure) updateStateWithLock(state procedure.State) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.state = state
}
