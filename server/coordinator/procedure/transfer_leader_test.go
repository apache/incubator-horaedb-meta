// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/dispatch"
	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/CeresDB/ceresmeta/server/schedule"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	nodeName0                = "node0"
	nodeName1                = "node1"
	testRootPath             = "/rootPath"
	defaultIDAllocatorStep   = 20
	clusterName              = "ceresdbCluster1"
	defaultNodeCount         = 2
	defaultReplicationFactor = 1
	defaultShardTotal        = 2
)

func TestTransferLeader(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	dispatch := dispatch.NewEventDispatchImpl()
	cluster := newTestCluster(ctx, t)

	// Initialize shard topology
	shardVies := make([]*clusterpb.Shard, 0)
	shard0 := &clusterpb.Shard{
		ShardRole: clusterpb.ShardRole_LEADER,
		Node:      nodeName0,
		Id:        0,
	}
	shardVies = append(shardVies, shard0)
	shard1 := &clusterpb.Shard{
		ShardRole: clusterpb.ShardRole_LEADER,
		Node:      nodeName0,
		Id:        1,
	}
	shardVies = append(shardVies, shard1)
	err := cluster.UpdateClusterTopology(ctx, clusterpb.ClusterTopology_STABLE, shardVies)
	re.NoError(err)

	// Create transfer leader procedure, oldLeader is shard0, newLeader is shard0 move to another node
	oldLeader := &clusterpb.Shard{
		ShardRole: clusterpb.ShardRole_LEADER,
		Node:      nodeName0,
		Id:        0,
	}
	newLeader := &clusterpb.Shard{
		ShardRole: clusterpb.ShardRole_LEADER,
		Node:      nodeName1,
		Id:        2,
	}
	procedure := NewTransferLeaderProcedure(dispatch, cluster, oldLeader, newLeader, uint64(1))
	err = procedure.Start(ctx)
	re.NoError(err)
	shardViews, err := cluster.GetClusterShardView()
	re.NoError(err)
	re.Equal(len(shardViews), 2)
	for _, shardView := range shardViews {
		shardID := shardView.GetId()
		if shardID == 2 {
			node := shardView.GetNode()
			re.Equal(nodeName1, node)
		}
	}
}

func newTestEtcdStorage(t *testing.T) (storage.Storage, clientv3.KV, etcdutil.CloseFn) {
	_, client, closeSrv := etcdutil.PrepareEtcdServerAndClient(t)
	storage := storage.NewStorageWithEtcdBackend(client, testRootPath, storage.Options{
		MaxScanLimit: 100, MinScanLimit: 10,
	})
	return storage, client, closeSrv
}

func newTestCluster(ctx context.Context, t *testing.T) *cluster.Cluster {
	re := require.New(t)
	storage, kv, _ := newTestEtcdStorage(t)
	manager, err := cluster.NewManagerImpl(storage, kv, schedule.NewHeartbeatStreams(ctx), testRootPath, defaultIDAllocatorStep)
	re.NoError(err)

	cluster, err := manager.CreateCluster(ctx, clusterName, defaultNodeCount, defaultReplicationFactor, defaultShardTotal)
	re.NoError(err)
	return cluster
}
