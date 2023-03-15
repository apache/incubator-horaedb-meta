package scheduler

import (
	"context"
	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/storage"
	"go.uber.org/zap"
	"sync"
)

// Manager used to manage schedulers.
// Each registered scheduler will generate a procedure if the cluster s
type Manager interface {
	RegisterScheduler(scheduler Scheduler)

	ListScheduler() []Scheduler

	// Scheduler will be called when received new heartbeat, every scheduler register in schedulerManager will be
	// Scheduler cloud be schedule with fix time interval or heartbeat.
	Scheduler(clusterView storage.ClusterView) []procedure.Procedure
}

type ManagerImpl struct {
	lock               sync.RWMutex
	registerSchedulers []Scheduler

	procedureManager procedure.Manager

	heartbeatChan chan *metaservicepb.NodeInfo
}

func (m *ManagerImpl) Start(ctx context.Context) error {
	go func() {
		// Get latest clusterView from heartbeat.

		// Get lateset shardViews from metadata
		m.Scheduler(ctx, nil, nil)
	}()

	return nil
}

func (m *ManagerImpl) RegisterScheduler(scheduler Scheduler) {
	m.lock.Lock()
	defer m.lock.Unlock()

	log.Info("register new scheduler", zap.Int("schedulerLen", len(m.registerSchedulers)))
	m.registerSchedulers = append(m.registerSchedulers, scheduler)
}

func (m *ManagerImpl) ListScheduler() []Scheduler {
	m.lock.RLock()
	m.lock.RUnlock()

	return m.registerSchedulers
}

func (m *ManagerImpl) Scheduler(ctx context.Context, clusterView storage.ClusterView, shardViews []storage.ShardView) []procedure.Procedure {
	for _, scheduler := range m.registerSchedulers {
		log.Info("scheduler start")
		err, p, desc := scheduler.Schedule(ctx, clusterView, shardViews)
		if err != nil {
			log.Info("scheduler new procedure", zap.String("desc", desc))
			err := m.procedureManager.Submit(ctx, p)
			if err != nil {
				log.Error("scheduler submit new procedure", zap.Uint64("ProcedureID", p.ID()), zap.Error(err))
			}
		}
	}
	return nil
}
