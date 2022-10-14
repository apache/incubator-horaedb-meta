// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"sync"

	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/id"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type ManagerImpl struct {
	lock       sync.RWMutex
	procedures []Procedure

	storage  Storage
	cluster  *cluster.Cluster
	dispatch eventdispatch.Dispatch

	procedureChan        chan<- Procedure
	procedureIDAllocator id.Allocator
	shardIDAllocator     id.Allocator
}

func (m *ManagerImpl) SubmitScatterTask(cxt context.Context, _ *ScatterRequest) error {
	procedureID, err := m.procedureIDAllocator.Alloc(cxt)
	if err != nil {
		return errors.WithMessage(err, "alloc procedure id failed")
	}
	procedure := NewScatterProcedure(m.dispatch, m.cluster, procedureID, m.shardIDAllocator)
	return m.submit(cxt, procedure)
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

func NewManagerImpl(ctx context.Context, cluster *cluster.Cluster, client *clientv3.Client, rootPath string) (Manager, error) {
	manager := &ManagerImpl{
		storage:              NewEtcdStorageImpl(client, cluster.GetClusterID(), rootPath),
		cluster:              cluster,
		dispatch:             eventdispatch.NewDispatchImpl(),
		shardIDAllocator:     id.NewReusableAllocatorImpl(make([]uint64, 0), 0),
		procedureIDAllocator: id.NewAllocatorImpl(client, "procedure", 5),
	}

	err := manager.Init(ctx)
	if err != nil {
		return nil, errors.WithMessage(err, "init manager failed")
	}

	return manager, nil
}

func (m *ManagerImpl) Init(ctx context.Context) error {
	procedureChan := make(chan Procedure, 10)
	m.procedureChan = procedureChan
	go startProcedureWorker(ctx, procedureChan, make(chan error, 10))
	if err := m.SubmitScatterTask(ctx, &ScatterRequest{}); err != nil {
		return errors.WithMessage(err, "submit scatter task failed")
	}
	err := m.retryAll(ctx)
	if err != nil {
		return errors.WithMessage(err, "retry all running procedure failed")
	}
	return nil
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

func startProcedureWorker(ctx context.Context, procedures <-chan Procedure, results chan<- error) {
	for procedure := range procedures {
		err := procedure.Start(ctx)
		results <- err
	}
}

func (m *ManagerImpl) submit(_ context.Context, procedure Procedure) error {
	m.procedureChan <- procedure
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
