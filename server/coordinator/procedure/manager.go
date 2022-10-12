// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"sync"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/dispatch"
	"github.com/pkg/errors"
)

type Manager interface {
	SubmitScatterTask(ctx context.Context, request *ScatterRequest) error
	SubmitTransferLeaderTask(ctx context.Context, request *TransferLeaderRequest) error
	SubmitMigrateTask(ctx context.Context, request *MigrateRequest) error
	SubmitSplitTask(ctx context.Context, request *SplitRequest) error
	SubmitMergeTask(ctx context.Context, request *MergeRequest) error

	Cancel(ctx context.Context, procedureID uint64) error
	ListRunningProcedure(ctx context.Context) ([]*RunningProcedureInfo, error)
}

type ScatterRequest struct {
	nodeInfo *metaservicepb.NodeInfo
}

type TransferLeaderRequest struct {
	oldLeader *clusterpb.Shard
	newLeader *clusterpb.Shard
}

type MigrateRequest struct {
	targetShard *clusterpb.Shard
	targetNode  *clusterpb.Node
}

type SplitRequest struct {
	targetShard *clusterpb.Shard
}

type MergeRequest struct {
	targetShards []*clusterpb.Shard
}

type ManagerImpl struct {
	lock       sync.RWMutex
	procedures []Procedure

	storage  Storage
	cluster  *cluster.Cluster
	dispatch dispatch.ActionDispatch
}

func (m *ManagerImpl) SubmitScatterTask(_ context.Context, _ *ScatterRequest) error {
	// TODO implement me
	panic("implement me")
}

func (m *ManagerImpl) SubmitTransferLeaderTask(_ context.Context, _ *TransferLeaderRequest) error {
	// TODO implement me
	panic("implement me")
}

func (m *ManagerImpl) SubmitMigrateTask(_ context.Context, _ *MigrateRequest) error {
	// TODO implement me
	panic("implement me")
}

func (m *ManagerImpl) SubmitSplitTask(_ context.Context, _ *SplitRequest) error {
	// TODO implement me
	panic("implement me")
}

func (m *ManagerImpl) SubmitMergeTask(_ context.Context, _ *MergeRequest) error {
	// TODO implement me
	panic("implement me")
}

func (m *ManagerImpl) Cancel(ctx context.Context, procedureID uint64) error {
	m.lock.RLock()
	defer m.lock.RUnlock()
	for _, procedure := range m.procedures {
		if procedure.ID() == procedureID {
			err := procedure.Cancel(ctx)
			return errors.WithMessagef(err, "cancel procedure failed, procedureID = %d", procedureID)
		}
	}
	return nil
}

func (m *ManagerImpl) ListRunningProcedure(_ context.Context) ([]*RunningProcedureInfo, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	procedureInfos := make([]*RunningProcedureInfo, 0)
	for _, procedure := range m.procedures {
		procedureInfos = append(procedureInfos, &RunningProcedureInfo{
			ID:    procedure.ID(),
			Typ:   procedure.Typ(),
			State: procedure.State(),
		})
	}
	return procedureInfos, nil
}

type RunningProcedureInfo struct {
	ID    uint64
	Typ   Typ
	State State
}

func NewManagerImpl(ctx context.Context, storage Storage, cluster *cluster.Cluster, actionDispatch dispatch.ActionDispatch) (Manager, error) {
	manager := &ManagerImpl{
		storage:  storage,
		cluster:  cluster,
		dispatch: actionDispatch,
	}

	err := manager.retryAll(ctx)
	if err != nil {
		return nil, errors.WithMessage(err, "retry all running procedure failed")
	}
	return manager, nil
}

func (m *ManagerImpl) retryAll(ctx context.Context) error {
	metas, err := m.storage.List(ctx, 100)
	if err != nil {
		return errors.WithMessage(err, "storage list meta failed")
	}
	for _, meta := range metas {
		if !checkNeedRetry(meta) {
			continue
		}
		p := load(meta)
		err := m.retry(ctx, p)
		return errors.WithMessagef(err, "retry procedure failed, procedureID = %d", p.ID())
	}
	return nil
}

func (m *ManagerImpl) submit(ctx context.Context, procedure Procedure) error {
	err := procedure.Start(ctx)
	if err != nil {
		return errors.WithMessage(err, "submit procedure failed")
	}
	return nil
}

func (m *ManagerImpl) retry(ctx context.Context, procedure Procedure) error {
	err := procedure.Start(ctx)
	if err != nil {
		return errors.WithMessagef(err, "start procedure failed, procedureID = %d", procedure.ID())
	}
	return nil
}

// load meta and restore procedure
func load(meta *Meta) Procedure {
	typ := meta.Typ
	rawData := meta.RawData
	procedure := RestoreProcedure(typ, rawData)
	return procedure
}

func RestoreProcedure(operationType Typ, _ []byte) Procedure {
	switch operationType {
	case Create:
		return nil
	case Delete:
		return nil
	case TransferLeader:
		return nil
	case Migrate:
		return nil
	case Split:
		return nil
	case Merge:
		return nil
	case Scatter:
		return nil
	}
	return nil
}

func checkNeedRetry(meta *Meta) bool {
	state := meta.State
	if state == StateCancelled || state == StateFinished {
		return false
	}
	return true
}
