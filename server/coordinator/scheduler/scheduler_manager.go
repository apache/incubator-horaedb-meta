// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package scheduler

import (
	"context"
	"fmt"
	"github.com/CeresDB/ceresmeta/server/coordinator/watch"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"reflect"
	"sync"
	"time"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"go.uber.org/zap"
)

const (
	schedulerInterval = time.Second * 5
)

// Manager used to manage schedulers, it will register all schedulers when it starts.
//
// Each registered scheduler will generate procedures if the cluster topology matches the scheduling condition.
type Manager interface {
	ListScheduler() []Scheduler

	Start(ctx context.Context) error

	Stop(ctx context.Context) error

	// Scheduler will be called when received new heartbeat, every scheduler registered in schedulerManager will be called to generate procedures.
	// Scheduler cloud be schedule with fix time interval or heartbeat.
	Scheduler(ctx context.Context, clusterSnapshot cluster.Snapshot) []*ScheduleResult
}

type ManagerImpl struct {
	procedureManager procedure.Manager
	clusterManager   cluster.Manager
	factory          *coordinator.Factory
	nodePicker       coordinator.NodePicker
	client           *clientv3.Client
	rootPath         string

	// This lock is used to protect the following field.
	lock               sync.RWMutex
	registerSchedulers []Scheduler
	shardWatches       map[string]*watch.ShardWatch
	isRunning          bool
	cancel             context.CancelFunc
}

func NewManager(procedureManager procedure.Manager, clusterManager cluster.Manager, factory *coordinator.Factory, client *clientv3.Client, rootPath string) Manager {
	return &ManagerImpl{
		procedureManager:   procedureManager,
		clusterManager:     clusterManager,
		registerSchedulers: []Scheduler{},
		isRunning:          false,
		factory:            factory,
		nodePicker:         coordinator.NewRandomNodePicker(),
	}
}

func (m *ManagerImpl) Stop(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.isRunning {
		m.cancel()
		m.registerSchedulers = m.registerSchedulers[:0]
		m.isRunning = false
		for _, shardWatch := range m.shardWatches {
			if err := shardWatch.Stop(ctx); err != nil {
				return errors.WithMessage(err, "stop shard watch failed")
			}
		}
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
		watch := watch.NewWatch(c.Name(), m.rootPath, m.client)
		watch.RegisteringEventCallback(&schedulerWatchCallback{c: c})
		m.shardWatches[c.Name()] = watch
		if err := watch.Start(ctx); err != nil {
			return errors.WithMessage(err, "start shard watch failed")
		}
		go func() {
			for {
				select {
				case <-ctxWithCancel.Done():
					log.Info("scheduler manager is canceled")
					return
				default:
					time.Sleep(schedulerInterval)
					// Get latest cluster snapshot.
					clusterSnapshot := c.GetClusterSnapshot()
					log.Debug("scheduler manager invoke", zap.String("clusterSnapshot", fmt.Sprintf("%v", clusterSnapshot)))
					results := m.Scheduler(ctxWithCancel, clusterSnapshot)
					for _, result := range results {
						log.Info("scheduler submit new procedure", zap.Uint64("ProcedureID", result.Procedure.ID()), zap.String("Reason", result.Reason))
						if err := m.procedureManager.Submit(ctx, c.GetClusterID(), result.Procedure); err != nil {
							log.Error("scheduler submit new procedure failed", zap.Uint64("ProcedureID", result.Procedure.ID()), zap.Error(err))
						}
					}
				}
			}
		}()
	}

	m.isRunning = true
	m.cancel = cancel

	return nil
}

type schedulerWatchCallback struct {
	c *cluster.Cluster
}

func (callback *schedulerWatchCallback) OnShardRegistered(event watch.ShardRegisterEvent) error {
	return nil
}

func (callback *schedulerWatchCallback) OnShardExpired(event watch.ShardExpireEvent) error {
	return nil
}

// Schedulers should to be initialized and registered here.
func (m *ManagerImpl) initRegister() {
	assignShardScheduler := NewAssignShardScheduler(m.factory, m.nodePicker)
	m.registerScheduler(assignShardScheduler)
}

func (m *ManagerImpl) registerScheduler(scheduler Scheduler) {
	log.Info("register new scheduler", zap.String("schedulerName", reflect.TypeOf(scheduler).String()), zap.Int("totalSchedulerLen", len(m.registerSchedulers)))
	m.registerSchedulers = append(m.registerSchedulers, scheduler)
}

func (m *ManagerImpl) ListScheduler() []Scheduler {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.registerSchedulers
}

func (m *ManagerImpl) Scheduler(ctx context.Context, clusterSnapshot cluster.Snapshot) []*ScheduleResult {
	// TODO: Every scheduler should run in an independent goroutine.
	var results []*ScheduleResult
	for _, scheduler := range m.registerSchedulers {
		result, err := scheduler.Schedule(ctx, clusterSnapshot)
		if err != nil {
			log.Error("scheduler failed", zap.Error(err))
			continue
		}
		if result != nil {
			results = append(results, result)
		}
	}
	return results
}
