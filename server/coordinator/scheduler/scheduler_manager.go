package scheduler

import (
	"context"
	"fmt"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/storage"
	"go.uber.org/zap"
	"sync"
	"time"
)

const (
	schedulerInterval = time.Second * 5
)

// Manager used to manage schedulers.
// Each registered scheduler will generate a procedure if the cluster s
type Manager interface {
	RegisterScheduler(scheduler Scheduler)

	ListScheduler() []Scheduler

	Start(ctx context.Context) error

	Stop(ctx context.Context) error

	// Scheduler will be called when received new heartbeat, every scheduler register in schedulerManager will be
	// Scheduler cloud be schedule with fix time interval or heartbeat.
	Scheduler(ctx context.Context, clusterView storage.ClusterView, shardViews []storage.ShardView) []procedure.Procedure
}

type ManagerImpl struct {
	lock               sync.RWMutex
	registerSchedulers []Scheduler
	procedureManager   procedure.Manager

	clusterManager cluster.Manager
}

func NewManager(procedureManager procedure.Manager, clusterManager cluster.Manager) Manager {
	return &ManagerImpl{
		procedureManager: procedureManager,
		clusterManager:   clusterManager,
	}
}

func (m *ManagerImpl) Stop(ctx context.Context) error {
	return nil
}

func (m *ManagerImpl) Start(ctx context.Context) error {
	clusters, err := m.clusterManager.ListClusters(ctx)
	if err != nil {
		return err
	}
	for _, cluster := range clusters {
		cluster := cluster
		go func() {
			for {
				time.Sleep(schedulerInterval)
				// Get latest clusterView from heartbeat.
				clusterView := cluster.GetClusterView()
				// Get latest shardViews from metadata
				shardViews := cluster.GetShardViews()
				log.Info("scheduler manager invoke", zap.String("clusterView", fmt.Sprintf("%v", clusterView)), zap.String("shardViews", fmt.Sprintf("%v", shardViews)))
				m.Scheduler(ctx, clusterView, shardViews)
			}
		}()
	}

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
