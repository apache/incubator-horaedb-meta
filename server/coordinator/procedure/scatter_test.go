// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
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
	defaultShardTotal        = 8
)

func TestScatter(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	dispatch := dispatch.NewEventDispatchImpl()
	cluster := newTestCluster(ctx, t)

	nodeInfo1 := &metaservicepb.NodeInfo{
		Endpoint:   nodeName0,
		ShardInfos: nil,
	}

	// Cluster is empty, it should be return and do nothing
	err := cluster.RegisterNode(ctx, nodeInfo1)
	re.NoError(err)
	p := NewScatterProcedure(dispatch, cluster, nodeInfo1)
	err = p.Start(ctx)
	re.NoError(err)
	re.Equal(clusterpb.ClusterTopology_EMPTY, cluster.GetClusterState())

	// Register two node, defaultNodeCount is satisfy, Initialize shard topology
	nodeInfo2 := &metaservicepb.NodeInfo{
		Endpoint:   nodeName1,
		ShardInfos: nil,
	}
	err = cluster.RegisterNode(ctx, nodeInfo2)
	re.NoError(err)
	p = NewScatterProcedure(dispatch, cluster, nodeInfo2)
	err = p.Start(ctx)
	re.NoError(err)
	re.Equal(clusterpb.ClusterTopology_STABLE, cluster.GetClusterState())
	shardViews, err := cluster.GetClusterShardView()
	re.NoError(err)
	re.Equal(len(shardViews), defaultShardTotal)
	shardNodeMapping := make(map[string][]uint32, 0)
	for _, shardView := range shardViews {
		nodeName := shardView.GetNode()
		shardID := shardView.GetId()
		_, exists := shardNodeMapping[nodeName]
		if !exists {
			shardNodeMapping[nodeName] = make([]uint32, 0)
		}
		shardNodeMapping[nodeName] = append(shardNodeMapping[nodeName], shardID)
	}
	// Cluster shard topology should be initialized, shard length in every node should be defaultShardTotal/defaultNodeCount
	re.Equal(len(shardNodeMapping), defaultNodeCount)
	re.Equal(len(shardNodeMapping[nodeName0]), defaultShardTotal/defaultNodeCount)
	re.Equal(len(shardNodeMapping[nodeName1]), defaultShardTotal/defaultNodeCount)
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
