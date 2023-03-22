// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"fmt"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/lock"
	"github.com/CeresDB/ceresmeta/server/storage"
	"sync"
	"time"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	metaListBatchSize        = 100
	defaultWaitingQueueSize  = 100
	defaultWaitingQueueDelay = time.Millisecond * 500
)

type ManagerImpl struct {
	storage        Storage
	dispatch       eventdispatch.Dispatch
	clusterManager cluster.Manager

	// This lock is used to protect the following fields.
	lock     sync.RWMutex
	running  bool
	clusters map[storage.ClusterID]*ClusterProcedures
}

type ClusterProcedures struct {
	c *cluster.Cluster
	// There is only one procedure running for every shard.
	// It will be removed when the procedure is finished or failed.
	runningProcedures map[storage.ShardID]Procedure
	// ProcedureShardLock used to manager procedure related with multi shard, which need to grant all shard locks to be running procedure.
	procedureShardLock lock.EntryLock
	// All procedure will be put into waiting queue first, when runningProcedure is empty, try to promote some waiting procedures to new running procedures.
	waitingProcedures *ProcedureDelayQueue
}

func (m *ManagerImpl) Start(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.running {
		log.Warn("cluster manager has already been started")
		return nil
	}

	clusters, err := m.clusterManager.ListClusters(ctx)
	if err != nil {
		log.Error("list cluster failed")
	}
	for _, c := range clusters {
		if _, exists := m.clusters[c.GetClusterID()]; exists {
			continue
		}
		procedures := &ClusterProcedures{
			c:                  c,
			procedureShardLock: lock.NewEntryLock(10),
			runningProcedures:  map[storage.ShardID]Procedure{},
			waitingProcedures:  NewProcedureDelayQueue(defaultWaitingQueueSize),
		}
		m.clusters[c.GetClusterID()] = procedures
		log.Info("start cluster worker", zap.String("clusterName", c.Name()))
		go m.startProcedurePromote(ctx, procedures)
		go m.startProcedureWorker(ctx, procedures)
	}

	return nil
}

func (m *ManagerImpl) Stop(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	for _, clusterProcedures := range m.clusters {
		for _, procedure := range clusterProcedures.runningProcedures {
			if procedure.State() == StateRunning {
				err := procedure.Cancel(ctx)
				log.Error("cancel procedure failed", zap.Error(err), zap.Uint64("procedureID", procedure.ID()))
				// TODO: consider whether a single procedure cancel failed should return directly.
				return err
			}
		}
	}
	return nil
}

func (m *ManagerImpl) Submit(_ context.Context, clusterID storage.ClusterID, procedure Procedure) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, exists := m.clusters[clusterID]; !exists {
		return errors.WithMessage(cluster.ErrClusterNotFound, fmt.Sprintf("cluster not found, clusterID = %d", clusterID))
	}

	return m.clusters[clusterID].waitingProcedures.Push(procedure, 0)
}

func (m *ManagerImpl) ListRunningProcedure(_ context.Context, clusterID storage.ClusterID) ([]*Info, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if _, exists := m.clusters[clusterID]; !exists {
		return nil, errors.WithMessage(cluster.ErrClusterNotFound, fmt.Sprintf("cluster not found, clusterID = %d", clusterID))
	}

	var procedureInfos []*Info
	for _, procedure := range m.clusters[clusterID].runningProcedures {
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

func NewManagerImpl(s Storage, clusterManager cluster.Manager) (Manager, error) {
	manager := &ManagerImpl{
		storage:        s,
		clusterManager: clusterManager,
		dispatch:       eventdispatch.NewDispatchImpl(),
		clusters:       map[storage.ClusterID]*ClusterProcedures{},
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

func (m *ManagerImpl) startProcedurePromote(ctx context.Context, procedures *ClusterProcedures) {
	for {
		err, newProcedures := m.promoteProcedure(ctx, procedures)
		if err != nil {
			log.Error("promote procedure failed", zap.Error(err))
			continue
		}
		if len(newProcedures) > 0 {
			for _, newProcedure := range newProcedures {
				log.Info("promote procedure", zap.Uint64("procedureID", newProcedure.ID()))
				for shardID := range newProcedure.RelatedVersionInfo().ShardWithVersion {
					procedures.runningProcedures[shardID] = newProcedure
				}

				newProcedure := newProcedure
				go func() {
					log.Info("procedure start", zap.Uint64("procedureID", newProcedure.ID()))
					err := newProcedure.Start(ctx)
					if err != nil {
						log.Error("procedure start failed", zap.Error(err))
					}
				}()
			}
		}
	}
}

func (m *ManagerImpl) startProcedureWorker(ctx context.Context, procedures *ClusterProcedures) {
	for {
		for _, procedure := range procedures.runningProcedures {
			p := procedure
			if p.State() == StateFailed || p.State() == StateCancelled || p.State() == StateFinished {
				log.Info("procedure finish", zap.Uint64("procedureID", p.ID()))
				for shardID := range p.RelatedVersionInfo().ShardWithVersion {
					delete(procedures.runningProcedures, shardID)
					procedures.procedureShardLock.UnLock([]uint64{uint64(shardID)})
				}
			}
		}
	}
}

func (m *ManagerImpl) retry(ctx context.Context, procedure Procedure) error {
	err := procedure.Start(ctx)
	if err != nil {
		return errors.WithMessagef(err, "start procedure failed, procedureID:%d", procedure.ID())
	}
	return nil
}

// Whether a waiting procedure could be running procedure.
func (m *ManagerImpl) checkValid(ctx context.Context, procedure Procedure, cluster *cluster.Cluster) bool {
	// ClusterVersion and ShardVersion in this procedure must be same with current cluster topology.
	return true
}

// Promote a waiting procedure to be a running procedure.
// Some procedure may be related with multi shards.
func (m *ManagerImpl) promoteProcedure(ctx context.Context, procedures *ClusterProcedures) (error, []Procedure) {
	// Get waiting procedures, it has been sorted in queue.
	queue := procedures.waitingProcedures

	var result []Procedure
	// Find next valid procedure.
	for {
		p := queue.Pop()
		if p == nil {
			return nil, result
		}

		if !m.checkValid(ctx, p, procedures.c) {
			// This procedure is invalid, just remove it.
			continue
		}

		// Try to get shard locks.
		var shardIDs []uint64
		for shardID := range p.RelatedVersionInfo().ShardWithVersion {
			shardIDs = append(shardIDs, uint64(shardID))
		}
		lockResult := procedures.procedureShardLock.TryLock(shardIDs)
		if lockResult {
			// Get lock success, procedure will be executed.
			result = append(result, p)
		} else {
			// Get lock failed, procedure will be put back into the queue.
			if err := queue.Push(p, defaultWaitingQueueDelay); err != nil {
				return err, nil
			}
		}
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
