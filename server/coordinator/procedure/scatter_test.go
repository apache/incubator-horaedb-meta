// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/dispatch"
	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/CeresDB/ceresmeta/server/id"
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

	allocator := id.NewReusableAllocatorImpl(make([]uint64, 0), 0)
	p := NewScatterProcedure(dispatch, cluster, 1, allocator)
	go func() {
		err := p.Start(ctx)
		re.NoError(err)
	}()

	// Cluster is empty, it should be return and do nothing
	err := cluster.RegisterNode(ctx, nodeInfo1)
	re.NoError(err)
	re.Equal(clusterpb.ClusterTopology_EMPTY, cluster.GetClusterState())

	// Register two node, defaultNodeCount is satisfied, Initialize shard topology
	nodeInfo2 := &metaservicepb.NodeInfo{
		Endpoint:   nodeName1,
		ShardInfos: nil,
	}
	err = cluster.RegisterNode(ctx, nodeInfo2)
	re.NoError(err)
	time.Sleep(time.Second * 3)
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

func TestAllocNodeShard(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()

	minNodeCount := 4
	shardTotal := 2
	nodeList := make([]*clusterpb.Node, 0)
	for i := 0; i < minNodeCount; i++ {
		nodeInfo := &clusterpb.Node{
			Name: fmt.Sprintf("node%d", i),
		}
		nodeList = append(nodeList, nodeInfo)
	}
	allocator := id.NewReusableAllocatorImpl(make([]uint64, 0), 0)
	// NodeCount = 4, shardTotal = 2
	// Two shard distributed in node0,node1
	shardView, err := allocNodeShards(ctx, uint32(shardTotal), uint32(minNodeCount), nodeList, allocator)
	re.NoError(err)
	re.Equal(shardTotal, len(shardView))
	re.Equal("node0", shardView[0].Node)
	re.Equal("node1", shardView[1].Node)

	minNodeCount = 2
	shardTotal = 3
	nodeList = make([]*clusterpb.Node, 0)
	for i := 0; i < minNodeCount; i++ {
		nodeInfo := &clusterpb.Node{
			Name: fmt.Sprintf("node%d", i),
		}
		nodeList = append(nodeList, nodeInfo)
	}
	// NodeCount = 2, shardTotal = 3
	// Three shard distributed in node0,node0,node1
	allocator = id.NewReusableAllocatorImpl(make([]uint64, 0), 0)
	shardView, err = allocNodeShards(ctx, uint32(shardTotal), uint32(minNodeCount), nodeList, allocator)
	re.NoError(err)
	re.Equal(shardTotal, len(shardView))
	re.Equal("node0", shardView[0].Node)
	re.Equal("node0", shardView[1].Node)
	re.Equal("node1", shardView[2].Node)
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
