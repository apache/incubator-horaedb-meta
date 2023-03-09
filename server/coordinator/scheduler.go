// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator

import (
	"context"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"sync"
	"time"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/storage"
)

const (
	heartbeatCheckInterval     = 10 * time.Second
	failoverRetryMaxSize       = 3
	failoverRetryInterval      = 10 * time.Second
	heartbeatChannelBufferSize = 1000
)

type Scheduler struct {
	// This lock is used to protect the field `running`.
	lock    sync.RWMutex
	running bool

	clusterManager   cluster.Manager
	procedureManager procedure.Manager
	procedureFactory *Factory
	dispatch         eventdispatch.Dispatch

	checkNodeTicker *time.Ticker

	ShardWatch ShardWatch

	heartbeatChan chan *metaservicepb.NodeInfo
}

func NewScheduler(clusterManager cluster.Manager, procedureManager procedure.Manager, procedureFactory *Factory, dispatch eventdispatch.Dispatch, rootPath string, etcdClient *clientv3.Client) *Scheduler {
	return &Scheduler{
		running:          false,
		clusterManager:   clusterManager,
		procedureManager: procedureManager,
		procedureFactory: procedureFactory,
		dispatch:         dispatch,
		ShardWatch: ShardWatch{
			rootPath:   rootPath,
			etcdClient: etcdClient,
		},
		heartbeatChan: make(chan *metaservicepb.NodeInfo, heartbeatChannelBufferSize),
	}
}

func (s *Scheduler) Start(ctx context.Context) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.running {
		log.Warn("scheduler has already been started")
		return nil
	}

	s.running = true
	ticker := time.NewTicker(heartbeatCheckInterval)
	s.checkNodeTicker = ticker
	if err := s.ShardWatch.registerWatch(ctx); err != nil {
		return err
	}
	s.ShardWatch.registerCallback(func(shardID storage.ShardID, nodeName string, eventType ShardEventType) error {
		switch eventType {
		case eventDelete:
			for retrySize := 0; retrySize < failoverRetryMaxSize; retrySize++ {
				err := s.failover(shardID, nodeName)
				if err != nil {
					// if failover failed, maybe is the node info is expired, try to retry after a while.
					log.Error("failover failed", zap.Error(err), zap.Int("retry size", retrySize))
					time.Sleep(failoverRetryInterval)
				}
			}
		case eventPut:
			// Do nothing
		}
		return nil
	})

	go func() {
		err := s.ProcessHeartbeat(ctx)
		if err != nil {
			log.Error("process heartbeat", zap.Error(err))
		}
	}()

	return nil
}

func (s *Scheduler) Stop(_ context.Context) error {
	s.checkNodeTicker.Stop()
	return nil
}

// ReceiveHeartbeat only receive heartbeat and put it into channel.
// If process heartbeat failed, what should we do?
func (s *Scheduler) ReceiveHeartbeat(_ context.Context, nodeInfo *metaservicepb.NodeInfo) {
	s.heartbeatChan <- nodeInfo
}

func (s *Scheduler) ProcessHeartbeat(ctx context.Context) error {
	for nodeInfo := range s.heartbeatChan {
		c, err := s.clusterManager.GetCluster(ctx, "defaultCluster")
		if err != nil {
			log.Error("get cluster", zap.Error(err))
			return err
		}

		if c.GetClusterState() != storage.ClusterStateStable {
			return nil
		}

		if err := c.UpdateTopologyByNodeInfo(ctx, nodeInfo); err != nil {
			log.Error("update topology by node info", zap.Error(err))
			return err
		}
	}
	return nil
}

func (s *Scheduler) failover(shardID storage.ShardID, nodeName string) error {
	// Pick Node and transfer leader.
	ctx := context.Background()
	nodes, err := s.clusterManager.ListRegisterNodes(ctx, "defaultCluster")
	if err != nil {
		return err
	}
	// Select another node to open shard.
	// TODO: replace the hard code with node picker, and try to retry transfer leader with another node when it failed.
	var newLeaderNode string
	for _, node := range nodes {
		if node.Node.Name != nodeName {
			newLeaderNode = node.Node.Name
		}
	}
	if len(newLeaderNode) == 0 {
		return procedure.ErrNodeNumberNotEnough
	}

	p, err := s.procedureFactory.CreateTransferLeaderProcedure(ctx, TransferLeaderRequest{
		ClusterName:       "defaultCluster",
		ShardID:           shardID,
		OldLeaderNodeName: nodeName,
		NewLeaderNodeName: newLeaderNode,
		ClusterVersion:    0,
	})
	if err != nil {
		return err
	}
	return s.procedureManager.Submit(ctx, p)
}
