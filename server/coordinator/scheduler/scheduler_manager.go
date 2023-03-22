// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package scheduler

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/storage"
	"go.uber.org/zap"
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
	procedureManager procedure.Manager
	clusterManager   cluster.Manager
	factory          coordinator.Factory
	nodePicker       coordinator.NodePicker

	// This lock is used to protect the following field.
	lock               sync.RWMutex
	registerSchedulers []Scheduler
	isRunning          bool
	cancel             context.CancelFunc
}

func NewManager(procedureManager procedure.Manager, clusterManager cluster.Manager) Manager {
	return &ManagerImpl{
		procedureManager:   procedureManager,
		clusterManager:     clusterManager,
		registerSchedulers: []Scheduler{},
		isRunning:          false,
	}
}

func (m *ManagerImpl) Stop(_ context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.isRunning {
		m.cancel()
		m.registerSchedulers = []Scheduler{}
		m.isRunning = false
	}

	return nil
}

func (m *ManagerImpl) Start(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.isRunning {
		return nil
	}

	m.initRegister()

	clusters, err := m.clusterManager.ListClusters(ctx)
	if err != nil {
		return err
	}
	ctxWithCancel, cancel := context.WithCancel(ctx)
	for _, c := range clusters {
		c := c
		go func() {
			for {
				time.Sleep(schedulerInterval)
				// Get latest clusterView from heartbeat.
				clusterView := c.GetClusterView()
				// Get latest shardViews from metadata
				shardViews := c.GetShardViews()
				log.Info("scheduler manager invoke", zap.String("clusterView", fmt.Sprintf("%v", clusterView)), zap.String("shardViews", fmt.Sprintf("%v", shardViews)))
				m.Scheduler(ctxWithCancel, clusterView, shardViews)
			}
		}()
	}

	m.isRunning = true
	m.cancel = cancel

	return nil
}

func (m *ManagerImpl) initRegister() {
	assignShardScheduler := NewAssignShardScheduler(m.factory, m.nodePicker)
	m.RegisterScheduler(assignShardScheduler)
}

func (m *ManagerImpl) RegisterScheduler(scheduler Scheduler) {
	m.lock.Lock()
	defer m.lock.Unlock()

	log.Info("register new scheduler", zap.String("schedulerName", reflect.TypeOf(scheduler).String()), zap.Int("totalSchedulerLen", len(m.registerSchedulers)))
	m.registerSchedulers = append(m.registerSchedulers, scheduler)
}

func (m *ManagerImpl) ListScheduler() []Scheduler {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.registerSchedulers
}

func (m *ManagerImpl) Scheduler(ctx context.Context, clusterView storage.ClusterView, shardViews []storage.ShardView) []procedure.Procedure {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			// TODO: Every scheduler should run in an independent goroutine.
			for _, scheduler := range m.registerSchedulers {
				log.Info("scheduler start")
				result, err := scheduler.Schedule(ctx, clusterView, shardViews)
				if err != nil {
					log.Info("scheduler new procedure", zap.String("desc", result.schedulerReason))
					err := m.procedureManager.Submit(ctx, result.p)
					if err != nil {
						log.Error("scheduler submit new procedure", zap.Uint64("ProcedureID", result.p.ID()), zap.Error(err))
					}
				}
			}
		}
	}
}
