// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"go.uber.org/zap"
	"sync"

	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	QueueSize         = 10
	MetaListBatchSize = 100
)

type ManagerImpl struct {
	// RWMutex is used to protect following fields.
	lock       sync.RWMutex
	procedures []Procedure

	storage  Storage
	dispatch eventdispatch.Dispatch

	procedureQueue     chan<- Procedure
	resultChannelQueue <-chan <-chan error
}

func (m *ManagerImpl) Start(ctx context.Context) error {
	procedureQueue := make(chan Procedure, QueueSize)
	m.procedureQueue = procedureQueue
	resultQueue := make(chan chan error, QueueSize)
	go startProcedureWorker(ctx, procedureQueue, resultQueue)
	err := m.retryAll(ctx)
	if err != nil {
		return errors.WithMessage(err, "retry all running procedure failed")
	}
	return nil
}

func (m *ManagerImpl) Stop(cxt context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, procedure := range m.procedures {
		if procedure.State() == StateRunning {
			err := procedure.Cancel(cxt)
			log.Error("cancel procedure failed", zap.Error(err), zap.Uint64("procedureID", procedure.ID()))
			// TODO: consider whether a single procedure cancel failed should return directly
			return err
		}
	}
	return nil
}

func (m *ManagerImpl) Submit(_ context.Context, procedure Procedure) (<-chan error, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.procedures = append(m.procedures, procedure)
	m.procedureQueue <- procedure
	resultChannel := <-m.resultChannelQueue
	return resultChannel, nil
}

func (m *ManagerImpl) Cancel(ctx context.Context, procedureID uint64) error {
	m.lock.RLock()
	defer m.lock.RUnlock()
	for _, procedure := range m.procedures {
		if procedure.ID() == procedureID {
			err := procedure.Cancel(ctx)
			return errors.WithMessagef(err, "cancel procedure failed, procedureID:%d", procedureID)
		}
	}
	return nil
}

func (m *ManagerImpl) ListRunningProcedure(_ context.Context) ([]*Info, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	procedureInfos := make([]*Info, 0)
	for _, procedure := range m.procedures {
		if procedure.State() != StateRunning {
			continue
		}
		procedureInfos = append(procedureInfos, &Info{
			ID:    procedure.ID(),
			Typ:   procedure.Typ(),
			State: procedure.State(),
		})
	}
	return procedureInfos, nil
}

func NewManagerImpl(cluster *cluster.Cluster, client *clientv3.Client, rootPath string) (Manager, error) {
	manager := &ManagerImpl{
		storage:  NewEtcdStorageImpl(client, cluster.GetClusterID(), rootPath),
		dispatch: eventdispatch.NewDispatchImpl(),
	}
	return manager, nil
}

func (m *ManagerImpl) retryAll(ctx context.Context) error {
	metas, err := m.storage.List(ctx, MetaListBatchSize)
	if err != nil {
		return errors.WithMessage(err, "storage list meta failed")
	}
	for _, meta := range metas {
		if !meta.needRetry() {
			continue
		}
		p := load(meta)
		err := m.retry(ctx, p)
		return errors.WithMessagef(err, "retry procedure failed, procedureID:%d", p.ID())
	}
	return nil
}

func startProcedureWorker(ctx context.Context, procedures <-chan Procedure, results chan chan error) {
	for procedure := range procedures {
		err := procedure.Start(ctx)
		resultChannel := make(chan error, 1)
		resultChannel <- err
		results <- resultChannel
	}
}

func (m *ManagerImpl) retry(ctx context.Context, procedure Procedure) error {
	err := procedure.Start(ctx)
	if err != nil {
		return errors.WithMessagef(err, "start procedure failed, procedureID:%d", procedure.ID())
	}
	return nil
}

// Load meta and restore procedure
func load(meta *Meta) Procedure {
	typ := meta.Typ
	rawData := meta.RawData
	procedure := restoreProcedure(typ, rawData)
	return procedure
}

func restoreProcedure(operationType Typ, _ []byte) Procedure {
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
