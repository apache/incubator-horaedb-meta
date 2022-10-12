// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/dispatch"
	"github.com/pkg/errors"
)

type Manager interface {
	SubmitScatterTask(ctx context.Context, nodeInfo *metaservicepb.NodeInfo) error
	SubmitTransferLeaderTask(ctx context.Context, oldLeader *clusterpb.Shard, newLeader *clusterpb.Shard) error
	SubmitMigrateTask(ctx context.Context, targetShard *clusterpb.Shard, targetNode *clusterpb.Node) error
	SubmitSplitTask(ctx context.Context, targetShard *clusterpb.Shard) error
	SubmitMergeTask(ctx context.Context, targetShards []*clusterpb.Shard) error

	Cancel(ctx context.Context, procedureID uint64) error
}

type ManagerImpl struct {
	storage    Storage
	procedures []Procedure
	cluster    *cluster.Cluster
	dispatch   dispatch.ActionDispatch
}

func (m ManagerImpl) SubmitScatterTask(_ context.Context, _ *metaservicepb.NodeInfo) error {
	// TODO implement me
	panic("implement me")
}

func (m ManagerImpl) SubmitTransferLeaderTask(_ context.Context, _ *clusterpb.Shard, _ *clusterpb.Shard) error {
	// TODO implement me
	panic("implement me")
}

func (m ManagerImpl) SubmitMigrateTask(_ context.Context, _ *clusterpb.Shard, _ *clusterpb.Node) error {
	// TODO implement me
	panic("implement me")
}

func (m ManagerImpl) SubmitSplitTask(_ context.Context, _ *clusterpb.Shard) error {
	// TODO implement me
	panic("implement me")
}

func (m ManagerImpl) SubmitMergeTask(_ context.Context, _ []*clusterpb.Shard) error {
	// TODO implement me
	panic("implement me")
}

func NewManagerImpl(storage Storage, cluster *cluster.Cluster, actionDispatch dispatch.ActionDispatch) (Manager, error) {
	return &ManagerImpl{
		storage:  storage,
		cluster:  cluster,
		dispatch: actionDispatch,
	}, nil
}

func (m ManagerImpl) RetryAll(ctx context.Context) error {
	metas, err := m.storage.List(ctx, 100)
	if err != nil {
		return errors.WithMessage(err, "storage list meta failed")
	}
	for _, meta := range metas {
		if !checkNeedRetry(meta) {
			continue
		}
		p := load(meta)
		err := m.Retry(ctx, p)
		return errors.WithMessagef(err, "retry procedure failed, procedureID = %d", p.ID())
	}
	return nil
}

func (m ManagerImpl) Submit(ctx context.Context, procedure Procedure) error {
	err := procedure.Start(ctx)
	if err != nil {
		return errors.WithMessage(err, "submit procedure failed")
	}
	return nil
}

func (m ManagerImpl) Cancel(ctx context.Context, procedureID uint64) error {
	for _, procedure := range m.procedures {
		if procedure.ID() == procedureID {
			err := procedure.Cancel(ctx)
			return errors.WithMessagef(err, "cancel procedure failed, procedureID = %d", procedureID)
		}
	}
	return nil
}

func (m ManagerImpl) Retry(ctx context.Context, procedure Procedure) error {
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
