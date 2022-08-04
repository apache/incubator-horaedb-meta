// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.
// TODO: add test cause in the future

package storage

import (
	"context"
	"path"
	"strconv"
	"testing"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"

	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func TestCluster(t *testing.T) {
	re := require.New(t)
	s := NewStorage(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()

	clusters := make([]*clusterpb.Cluster, 0)
	for i := 0; i < 20; i++ {
		cluster := &clusterpb.Cluster{Id: uint32(i), Name: "name", MinNodeCount: uint32(i), ReplicationFactor: uint32(i), ShardTotal: uint32(i)}
		cluster, err := s.CreateCluster(ctx, cluster)
		re.NoError(err)
		clusters = append(clusters, cluster)
	}
	values, err := s.ListClusters(ctx)
	re.NoError(err)
	for i := 0; i < 20; i++ {
		re.Equal(clusters[i].Id, values[i].Id)
		re.Equal(clusters[i].Name, values[i].Name)
		re.Equal(clusters[i].MinNodeCount, values[i].MinNodeCount)
		re.Equal(clusters[i].ReplicationFactor, values[i].ReplicationFactor)
		re.Equal(clusters[i].CreatedAt, values[i].CreatedAt)
		re.Equal(clusters[i].ShardTotal, values[i].ShardTotal)
	}
	// cluster := &clusterpb.Cluster{Id: uint32(0), Name: "name", MinNodeCount: uint32(1), ReplicationFactor: uint32(1), ShardTotal: uint32(1)}
	// err = s.PutCluster(ctx, uint32(0), cluster)
	// re.NoError(err)

	// value, err := s.GetCluster(ctx, uint32(0))
	// re.NoError(err)
	// re.Equal(cluster.Id, value.Id)
	// re.Equal(cluster.Name, value.Name)
	// re.Equal(cluster.MinNodeCount, value.MinNodeCount)
	// re.Equal(cluster.ReplicationFactor, value.ReplicationFactor)
	// re.Equal(cluster.CreatedAt, value.CreatedAt)
	// re.Equal(cluster.ShardTotal, value.ShardTotal)
}

func TestClusterTopology(t *testing.T) {
	re := require.New(t)
	s := NewStorage(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()

	clusterMetaData := &clusterpb.ClusterTopology{ClusterId: 1, DataVersion: 0, Cause: "cause"}
	clusterMetaData, err := s.CreateClusterTopology(ctx, clusterMetaData)
	re.NoError(err)

	value, err := s.GetClusterTopology(ctx, 1)
	re.NoError(err)
	re.Equal(clusterMetaData.ClusterId, value.ClusterId)
	re.Equal(clusterMetaData.DataVersion, value.DataVersion)
	re.Equal(clusterMetaData.Cause, value.Cause)
	re.Equal(clusterMetaData.CreatedAt, value.CreatedAt)

	clusterMetaData.DataVersion = uint64(1)
	err = s.PutClusterTopology(ctx, 1, 0, clusterMetaData)
	re.NoError(err)

	value, err = s.GetClusterTopology(ctx, 1)
	re.NoError(err)
	re.Equal(clusterMetaData.ClusterId, value.ClusterId)
	re.Equal(clusterMetaData.DataVersion, value.DataVersion)
	re.Equal(clusterMetaData.Cause, value.Cause)
	re.Equal(clusterMetaData.CreatedAt, value.CreatedAt)
}

func TestSchemes(t *testing.T) {
	re := require.New(t)
	s := NewStorage(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()

	schemas := make([]*clusterpb.Schema, 0)
	for i := 0; i < 10; i++ {
		schema := &clusterpb.Schema{Id: uint32(i), ClusterId: uint32(0), Name: "name"}
		schema, err := s.CreateSchema(ctx, uint32(0), schema)
		re.NoError(err)
		schemas = append(schemas, schema)
	}

	value, err := s.ListSchemas(ctx, 0)
	re.NoError(err)
	for i := 0; i < 10; i++ {
		re.Equal(schemas[i].Id, value[i].Id)
		re.Equal(schemas[i].ClusterId, value[i].ClusterId)
		re.Equal(schemas[i].Name, value[i].Name)
		re.Equal(schemas[i].CreatedAt, value[i].CreatedAt)
	}
	// for i := 0; i < 10; i++ {
	// 	schemas[i].Name = "name_"
	// }
	// err = s.PutSchemas(ctx, uint32(0), schemas)
	// re.NoError(err)

	// value, err = s.ListSchemas(ctx, 0)
	// re.NoError(err)
	// for i := 0; i < 10; i++ {
	// 	re.Equal(schemas[i].Id, value[i].Id)
	// 	re.Equal(schemas[i].ClusterId, value[i].ClusterId)
	// 	re.Equal(schemas[i].Name, value[i].Name)
	// 	re.Equal(schemas[i].CreatedAt, value[i].CreatedAt)
	// }
}

func TestTables(t *testing.T) {
	re := require.New(t)
	s := NewStorage(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()

	table_1 := &clusterpb.Table{Id: uint64(1), Name: "name_1", SchemaId: uint32(1), ShardId: uint32(1), Desc: "desc"}
	table_2 := &clusterpb.Table{Id: uint64(2), Name: "name_2", SchemaId: uint32(1), ShardId: uint32(1), Desc: "desc"}
	table_3 := &clusterpb.Table{Id: uint64(3), Name: "name_3", SchemaId: uint32(1), ShardId: uint32(1), Desc: "desc"}

	table_1, err := s.CreateTable(ctx, 1, 1, table_1)
	re.NoError(err)
	table_2, err = s.CreateTable(ctx, 1, 1, table_2)
	re.NoError(err)
	table_3, err = s.CreateTable(ctx, 1, 1, table_3)
	re.NoError(err)

	value, _, err := s.GetTable(ctx, 1, 1, "name_1")
	re.NoError(err)
	re.Equal(table_1.Id, value.Id)
	re.Equal(table_1.Name, value.Name)
	re.Equal(table_1.SchemaId, value.SchemaId)
	re.Equal(table_1.ShardId, value.ShardId)
	re.Equal(table_1.Desc, value.Desc)
	re.Equal(table_1.CreatedAt, value.CreatedAt)

	tables, err := s.ListTables(ctx, 1, 1)
	re.NoError(err)

	re.Equal(table_1.Id, tables[0].Id)
	re.Equal(table_1.Name, tables[0].Name)
	re.Equal(table_1.SchemaId, tables[0].SchemaId)
	re.Equal(table_1.ShardId, tables[0].ShardId)
	re.Equal(table_1.Desc, tables[0].Desc)
	re.Equal(table_1.CreatedAt, tables[0].CreatedAt)

	re.Equal(table_2.Id, tables[1].Id)
	re.Equal(table_2.Name, tables[1].Name)
	re.Equal(table_2.SchemaId, tables[1].SchemaId)
	re.Equal(table_2.ShardId, tables[1].ShardId)
	re.Equal(table_2.Desc, tables[1].Desc)
	re.Equal(table_2.CreatedAt, tables[1].CreatedAt)

	re.Equal(table_3.Id, tables[2].Id)
	re.Equal(table_3.Name, tables[2].Name)
	re.Equal(table_3.SchemaId, tables[2].SchemaId)
	re.Equal(table_3.ShardId, tables[2].ShardId)
	re.Equal(table_3.Desc, tables[2].Desc)
	re.Equal(table_3.CreatedAt, tables[2].CreatedAt)

	err = s.DeleteTable(ctx, 1, 1, "name_1")
	re.NoError(err)

}

// func TestShardTopologies(t *testing.T) {
// 	re := require.New(t)
// 	s := NewStorage(t)
// 	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
// 	defer cancel()

// 	for i := 0; i < 10; i++ {
// 		latestVersionKey := makeShardLatestVersionKey(0, uint32(i))
// 		err := s.Put(ctx, latestVersionKey, fmtID(uint64(0)))
// 		re.NoError(err)
// 	}

// 	shardTableInfo := make([]*clusterpb.ShardTopology, 0)
// 	shardID := make([]uint32, 0)
// 	for i := 0; i < 10; i++ {
// 		shardTableData := &clusterpb.ShardTopology{Version: 1}
// 		shardTableInfo = append(shardTableInfo, shardTableData)
// 		shardID = append(shardID, uint32(i))
// 	}

// 	err := s.PutShardTopologies(ctx, 0, shardID, 0, shardTableInfo)
// 	re.NoError(err)

// 	value, err := s.ListShardTopologies(ctx, 0, shardID)
// 	re.NoError(err)
// 	for i := 0; i < 10; i++ {
// 		re.Equal(shardTableInfo[i].Version, value[i].Version)
// 	}
// }

func TestNodes(t *testing.T) {
	re := require.New(t)
	s := NewStorage(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()

	node1 := &clusterpb.Node{Name: "127.0.0.1:8081"}
	node1, err := s.CreateOrUpdateNode(ctx, 1, node1)
	re.NoError(err)

	node2 := &clusterpb.Node{Name: "127.0.0.2:8081"}
	node2, err = s.CreateOrUpdateNode(ctx, 1, node2)
	re.NoError(err)

	node3 := &clusterpb.Node{Name: "127.0.0.3:8081"}
	node3, err = s.CreateOrUpdateNode(ctx, 1, node3)
	re.NoError(err)

	node4 := &clusterpb.Node{Name: "127.0.0.4:8081"}
	node4, err = s.CreateOrUpdateNode(ctx, 1, node4)
	re.NoError(err)

	node5 := &clusterpb.Node{Name: "127.0.0.5:8081"}
	node5, err = s.CreateOrUpdateNode(ctx, 1, node5)
	re.NoError(err)

	nodes, err := s.ListNodes(ctx, 1)
	re.NoError(err)

	re.Equal(node1.Name, nodes[0].Name)
	re.Equal(node1.CreateTime, nodes[0].CreateTime)
	re.Equal(node1.LastTouchTime, nodes[0].LastTouchTime)

	re.Equal(node2.Name, nodes[1].Name)
	re.Equal(node2.CreateTime, nodes[1].CreateTime)
	re.Equal(node2.LastTouchTime, nodes[1].LastTouchTime)

	re.Equal(node3.Name, nodes[2].Name)
	re.Equal(node3.CreateTime, nodes[2].CreateTime)
	re.Equal(node3.LastTouchTime, nodes[2].LastTouchTime)

	re.Equal(node4.Name, nodes[3].Name)
	re.Equal(node4.CreateTime, nodes[3].CreateTime)
	re.Equal(node4.LastTouchTime, nodes[3].LastTouchTime)

	re.Equal(node5.Name, nodes[4].Name)
	re.Equal(node5.CreateTime, nodes[4].CreateTime)
	re.Equal(node5.LastTouchTime, nodes[4].LastTouchTime)
}

func NewStorage(t *testing.T) Storage {
	cfg := etcdutil.NewTestSingleConfig()
	etcd, err := embed.StartEtcd(cfg)
	assert.NoError(t, err)

	<-etcd.Server.ReadyNotify()

	endpoint := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint},
	})
	assert.NoError(t, err)

	rootPath := path.Join("/ceresmeta", strconv.FormatUint(100, 10))
	ops := Options{MaxScanLimit: 100, MinScanLimit: 10}

	return newEtcdStorage(client, rootPath, ops)
}
