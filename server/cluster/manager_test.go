// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

const (
	cluster1                        = "ceresdbCluster1"
	cluster2                        = "ceresdbCluster2"
	defaultSchema                   = "ceresdbSchema"
	defaultNodeCount                = 2
	defaultReplicationFactor        = 1
	defaultShardTotal               = 8
	defaultLease                    = 100
	node1                           = "127.0.0.1:8081"
	node2                           = "127.0.0.2:8081"
	node3                           = "127.0.0.3:8081"
	node4                           = "127.0.0.4:8081"
	table1                          = "table1"
	table2                          = "table2"
	table3                          = "table3"
	table4                          = "table4"
	defaultSchemaID          uint32 = 0
	tableID1                 uint64 = 1
	tableID2                 uint64 = 2
	tableID3                 uint64 = 3
	tableID4                 uint64 = 4
)

func prepareEtcdServerAndClient(t *testing.T) (*embed.Etcd, *clientv3.Client, func()) {
	cfg := etcdutil.NewTestSingleConfig()
	etcd, err := embed.StartEtcd(cfg)
	assert.NoError(t, err)

	<-etcd.Server.ReadyNotify()

	endpoint := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint},
	})
	assert.NoError(t, err)

	clean := func() {
		etcd.Close()
		etcdutil.CleanConfig(cfg)
	}
	return etcd, client, clean
}

func newTestStorage(t *testing.T) storage.Storage {
	_, client, _ := prepareEtcdServerAndClient(t)
	storage := storage.NewStorageWithEtcdBackend(client, "/aaa", storage.Options{
		MaxScanLimit: 100, MinScanLimit: 10,
	})
	return storage
}

func newClusterManagerWithStorage(storage storage.Storage) (Manager, error) {
	return NewManagerImpl(context.Background(), storage)
}

func TestManager(t *testing.T) {
	re := require.New(t)
	storage := newTestStorage(t)
	manager, err := newClusterManagerWithStorage(storage)
	re.NoError(err)

	ctx := context.Background()
	testCreateCluster(ctx, re, manager, cluster1)

	testRegisterNode(ctx, re, manager, cluster1, node1, defaultLease)
	testRegisterNode(ctx, re, manager, cluster1, node2, defaultLease)

	testAllocSchemaID(ctx, re, manager, cluster1, defaultSchema, defaultSchemaID)
	testAllocSchemaID(ctx, re, manager, cluster1, defaultSchema, defaultSchemaID)

	testAllocTableID(ctx, re, manager, node1, cluster1, defaultSchema, table1, tableID1)
	testAllocTableID(ctx, re, manager, node1, cluster1, defaultSchema, table1, tableID1)
	testAllocTableID(ctx, re, manager, node1, cluster1, defaultSchema, table2, tableID2)
	testAllocTableID(ctx, re, manager, node2, cluster1, defaultSchema, table3, tableID3)
	testAllocTableID(ctx, re, manager, node2, cluster1, defaultSchema, table4, tableID4)

	testDropTable(ctx, re, manager, cluster1, defaultSchema, table1, tableID1)
	testDropTable(ctx, re, manager, cluster1, defaultSchema, table3, tableID3)

	testGetTables(ctx, re, manager, node1, cluster1)
	testGetTables(ctx, re, manager, node2, cluster1)

	manager, err = newClusterManagerWithStorage(storage)
	re.NoError(err)
	testGetTables(ctx, re, manager, node1, cluster1)
	testGetTables(ctx, re, manager, node2, cluster1)
}

func TestManagerWithMultiThread2(t *testing.T) {
	re := require.New(t)
	storage := newTestStorage(t)
	manager, err := newClusterManagerWithStorage(storage)
	re.NoError(err)

	ctx := context.Background()
	testCreateCluster(ctx, re, manager, cluster1)

	testRegisterNode(ctx, re, manager, cluster1, node1, defaultLease)
	testRegisterNode(ctx, re, manager, cluster1, node2, defaultLease)

	testAllocSchemaID(ctx, re, manager, cluster1, defaultSchema, defaultSchemaID)
	go testAllocSchemaID(ctx, re, manager, cluster1, defaultSchema, defaultSchemaID)

	testAllocTableIDWithMultiThread(ctx, re, manager, tableID1)
	testDropTable(ctx, re, manager, cluster1, defaultSchema, table1, tableID1)
	testAllocTableIDWithMultiThread(ctx, re, manager, tableID2)
}

func testAllocTableIDWithMultiThread(ctx context.Context, re *require.Assertions, manager Manager, tableID uint64) {
	go testAllocTableID(ctx, re, manager, node1, cluster1, defaultSchema, table1, tableID)
	testAllocTableID(ctx, re, manager, node2, cluster1, defaultSchema, table1, tableID)
}

func TestManagerWithMultiThread(t *testing.T) {
	re := require.New(t)
	storage := newTestStorage(t)
	manager, err := newClusterManagerWithStorage(storage)
	re.NoError(err)

	ctx := context.Background()

	go testCluster1(ctx, re, manager)
	testCluster2(ctx, re, manager)

}

func testCluster1(ctx context.Context, re *require.Assertions, manager Manager) {
	testCreateCluster(ctx, re, manager, cluster1)

	testRegisterNode(ctx, re, manager, cluster1, node1, defaultLease)
	testRegisterNode(ctx, re, manager, cluster1, node2, defaultLease)

	testAllocSchemaID(ctx, re, manager, cluster1, defaultSchema, defaultSchemaID)

	testAllocTableID(ctx, re, manager, node1, cluster1, defaultSchema, table1, tableID1)
	testAllocTableID(ctx, re, manager, node1, cluster1, defaultSchema, table1, tableID1)
	testAllocTableID(ctx, re, manager, node1, cluster1, defaultSchema, table2, tableID2)
	testAllocTableID(ctx, re, manager, node2, cluster1, defaultSchema, table3, tableID3)
	testAllocTableID(ctx, re, manager, node2, cluster1, defaultSchema, table4, tableID4)

	testGetTables(ctx, re, manager, node1, cluster1)
	testGetTables(ctx, re, manager, node2, cluster1)
}

func testCluster2(ctx context.Context, re *require.Assertions, manager Manager) {
	testCreateCluster(ctx, re, manager, cluster2)

	testRegisterNode(ctx, re, manager, cluster2, node3, defaultLease)
	testRegisterNode(ctx, re, manager, cluster2, node4, defaultLease)

	testAllocSchemaID(ctx, re, manager, cluster2, defaultSchema, defaultSchemaID)

	testAllocTableID(ctx, re, manager, node3, cluster2, defaultSchema, table1, tableID1)
	testAllocTableID(ctx, re, manager, node3, cluster2, defaultSchema, table1, tableID1)
	testAllocTableID(ctx, re, manager, node3, cluster2, defaultSchema, table2, tableID2)
	testAllocTableID(ctx, re, manager, node4, cluster2, defaultSchema, table3, tableID3)
	testAllocTableID(ctx, re, manager, node4, cluster2, defaultSchema, table4, tableID4)

	testGetTables(ctx, re, manager, node3, cluster2)
	testGetTables(ctx, re, manager, node4, cluster2)
}

func testCreateCluster(ctx context.Context, re *require.Assertions, manager Manager, clusterName string) {
	_, err := manager.CreateCluster(ctx, clusterName, defaultNodeCount, defaultReplicationFactor, defaultShardTotal)
	re.NoError(err)
}

func testRegisterNode(ctx context.Context, re *require.Assertions, manager Manager,
	cluster, node string, lease uint32,
) {
	err := manager.RegisterNode(ctx, cluster, node, lease)
	re.NoError(err)
}

func testAllocSchemaID(ctx context.Context, re *require.Assertions, manager Manager,
	cluster, schema string, schemaID uint32,
) {
	id, err := manager.AllocSchemaID(ctx, cluster, schema)
	re.NoError(err)
	re.Equal(schemaID, id)
}

func testAllocTableID(ctx context.Context, re *require.Assertions, manager Manager,
	node, cluster, schema, table string, tableID uint64,
) {
	id, err := manager.AllocTableID(ctx, cluster, schema, table, node)
	re.NoError(err)
	re.Equal(tableID, id)
}

func testGetTables(ctx context.Context, re *require.Assertions, manager Manager, node, cluster string) {
	shardIDs, err := manager.GetShards(ctx, cluster, node)
	re.NoError(err)

	shardTables, err := manager.GetTables(ctx, cluster, node, shardIDs)
	re.NoError(err)

	tableNum := 0
	for _, tables := range shardTables {
		re.Equal(clusterpb.ShardRole_LEADER, tables.shardRole)
		tableNum += len(tables.tables)
	}
	re.Equal(1, tableNum)
}

func testDropTable(ctx context.Context, re *require.Assertions, manager Manager, clusterName string, schemaName string, tableName string, tableID uint64) {
	err := manager.DropTable(ctx, clusterName, schemaName, tableName, tableID)
	re.NoError(err)
}
