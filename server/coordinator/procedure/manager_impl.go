// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"sync"
	"time"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	queueSize                   = 10
	metaListBatchSize           = 100
	defaultProcedureExpiredTime = time.Second * 5
)

type ManagerImpl struct {
	// This lock is used to protect the field `procedures` and `running`.
	lock       sync.RWMutex
	procedures []Procedure
	running    bool

	storage  Storage
	dispatch eventdispatch.Dispatch

	procedureQueue chan Procedure

	// There is only one procedure running for every shard.
	// It will be removed when the procedure is finished or failed.
	runningProcedures map[uint64]Procedure

	// All procedure will be put into waiting queue first,
	waitingProcedures map[uint64]WaitingProcedureQueue
}

func (m *ManagerImpl) Start(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.running {
		log.Warn("cluster manager has already been started")
		return nil
	}
	m.procedureQueue = make(chan Procedure, queueSize)
	go m.startProcedurePromote(ctx)
	go m.startProcedureWorker(ctx)
	err := m.retryAll(ctx)
	if err != nil {
		return errors.WithMessage(err, "retry all running procedure failed")
	}
	return nil
}

func (m *ManagerImpl) Stop(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	close(m.procedureQueue)
	for _, procedure := range m.procedures {
		if procedure.State() == StateRunning {
			err := procedure.Cancel(ctx)
			log.Error("cancel procedure failed", zap.Error(err), zap.Uint64("procedureID", procedure.ID()))
			// TODO: consider whether a single procedure cancel failed should return directly.
			return err
		}
	}
	return nil
}

func (m *ManagerImpl) Submit(_ context.Context, procedure Procedure) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	return m.waitingProcedures[procedure.ID()].Add(procedure)
}

func (m *ManagerImpl) Cancel(ctx context.Context, procedureID uint64) error {
	procedure := m.removeProcedure(procedureID)
	if procedure == nil {
		log.Error("procedure not found", zap.Uint64("procedureID", procedureID))
		return ErrProcedureNotFound
	}
	err := procedure.Cancel(ctx)
	if err != nil {
		return errors.WithMessagef(ErrProcedureNotFound, "cancel procedure failed, procedureID:%d", procedureID)
	}
	return nil
}

func (m *ManagerImpl) ListRunningProcedure(_ context.Context) ([]*Info, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	var procedureInfos []*Info
	for _, procedure := range m.procedures {
		if procedure.State() == StateRunning {
			procedureInfos = append(procedureInfos, &Info{
				ID:    procedure.ID(),
				Typ:   procedure.Typ(),
				State: procedure.State(),
			})
		}
	}
	return procedureInfos, nil
}

func NewManagerImpl(storage Storage) (Manager, error) {
	manager := &ManagerImpl{
		storage:  storage,
		dispatch: eventdispatch.NewDispatchImpl(),
	}
	return manager, nil
}

func (m *ManagerImpl) retryAll(ctx context.Context) error {
	metas, err := m.storage.List(ctx, metaListBatchSize)
	if err != nil {
		return errors.WithMessage(err, "storage list meta failed")
	}
	for _, meta := range metas {
		if !meta.needRetry() {
			continue
		}
		p := restoreProcedure(meta)
		if p != nil {
			err := m.retry(ctx, p)
			return errors.WithMessagef(err, "retry procedure failed, procedureID:%d", p.ID())
		}
	}
	return nil
}

func (m *ManagerImpl) startProcedurePromote(ctx context.Context) {
	for {
		for shardID, old := range m.runningProcedures {
			if old.State() == StateFailed || old.State() == StateFinished || old.State() == StateCancelled {
				delete(m.runningProcedures, shardID)
				new := m.promoteProcedure(ctx, shardID)
				m.runningProcedures[shardID] = new
			}
		}
	}
}

func (m *ManagerImpl) startProcedureWorker(ctx context.Context) {
	for _, procedure := range m.runningProcedures {
		p := procedure
		go func() {
			err := p.Start(ctx)
			if err != nil {
				log.Error("procedure start failed", zap.Error(err))
			}
		}()
	}
}

func (m *ManagerImpl) removeProcedure(id uint64) Procedure {
	m.lock.Lock()
	defer m.lock.Unlock()

	index := -1
	for i, p := range m.procedures {
		if p.ID() == id {
			index = i
			break
		}
	}
	if index != -1 {
		result := m.procedures[index]
		m.procedures = append(m.procedures[:index], m.procedures[index+1:]...)
		return result
	}
	return nil
}

func (m *ManagerImpl) retry(ctx context.Context, procedure Procedure) error {
	err := procedure.Start(ctx)
	if err != nil {
		return errors.WithMessagef(err, "start procedure failed, procedureID:%d", procedure.ID())
	}
	return nil
}

// Whether a waiting procedure could be running procedure.
func (m *ManagerImpl) checkValid(procedure Procedure) bool {
	return true
}

// Promote a waiting procedure to be a running procedure.
func (m *ManagerImpl) promoteProcedure(ctx context.Context, shardID uint64) Procedure {
	if m.runningProcedures[shardID] != nil {
		log.Warn("current running procedure do not finish, could not promote waiting procedure", zap.Uint64("shardID", shardID))
		return nil
	}

	// Get a waiting procedure, it has been sorted in queue.
	queue := m.waitingProcedures[shardID]

	for {
		p := queue.Deque()
		if p == nil {
			return nil
		}

		if !m.checkValid(p) {
			// This procedure is invalid, just cancel it.
			p.Cancel(ctx)
			continue
		}

		return p
	}
}

// Load meta and restore procedure.
// TODO: Restore procedure from metadata, some types of procedures do not need to be retried.
func restoreProcedure(meta *Meta) Procedure {
	switch meta.Typ {
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
	case CreateTable:
		return nil
	case DropTable:
		return nil
	case CreatePartitionTable:
		return nil
	case DropPartitionTable:
		return nil
	}
	return nil
}
