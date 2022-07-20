// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"context"
	"sync"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
)

type ShardRole int32

const (
	LEADER ShardRole = iota
	FOLLOWER
)

type TableInfo struct {
	id         uint64
	name       string
	schemaID   uint32
	schemaName string
}

type ShardTables struct {
	shardRole ShardRole
	tables    []*TableInfo
	version   uint64
}

type Manager interface {
	CreateCluster(ctx context.Context, clusterName string, nodeCount, replicationFactor, shardTotal uint32) (*Cluster, error)
	AllocSchemaID(ctx context.Context, clusterName, schemaName string) (uint32, error)
	AllocTableID(ctx context.Context, clusterName, schemaName, tableName, node string) (uint64, error)
	GetTables(ctx context.Context, clusterName, node string, shardIDs []uint32) (map[uint32]*ShardTables, error)
	DropTable(ctx context.Context, clusterName, schemaName, tableName string, tableID uint64) error
	RegisterNode(ctx context.Context, clusterName, node string, lease uint32) error
	GetShards(ctx context.Context, clusterName, node string) ([]uint32, error)
}

type ManagerImpl struct {
	sync.RWMutex
	cluster map[string]*Cluster
	storage storage.Storage
}

func NewManagerImpl(storage storage.Storage) *ManagerImpl {
	return &ManagerImpl{storage: storage, cluster: make(map[string]*Cluster, 0)}
}

func (m *ManagerImpl) Load(ctx context.Context) error {
	clusters, err := m.storage.ListClusters(ctx)
	if err != nil {
		return errors.Wrap(err, "cluster manager Load")
	}
	m.cluster = make(map[string]*Cluster, len(clusters))
	for _, clusterPb := range clusters {
		cluster := NewCluster(clusterPb, m.storage)
		if err := cluster.Load(ctx); err != nil {
			return errors.Wrapf(err, "cluster manager Load, cluster:%v", cluster)
		}
		m.cluster[cluster.Name()] = cluster
	}
	return nil
}

func (m *ManagerImpl) CreateCluster(ctx context.Context, clusterName string, nodeCount,
	replicationFactor, shardTotal uint32,
) (*Cluster, error) {
	m.Lock()
	defer m.Unlock()
	_, ok := m.cluster[clusterName]
	if ok {
		return nil, ErrClusterAlreadyExists
	}

	clusterID, err := m.allocClusterID()
	if err != nil {
		return nil, errors.Wrapf(err, "cluster manager CreateCluster, clusterName:%s", clusterName)
	}

	clusterPb := &clusterpb.Cluster{
		Id: clusterID, Name: clusterName, MinNodeCount: nodeCount,
		ReplicationFactor: replicationFactor, ShardTotal: shardTotal,
	}
	clusterPb, err = m.storage.CreateCluster(ctx, clusterPb)
	if err != nil {
		return nil, errors.Wrapf(err, "cluster manager CreateCluster, cluster:%v", clusterPb)
	}

	// todo: add scheduler
	clusterTopologyPb := &clusterpb.ClusterTopology{
		ClusterId: clusterID, DataVersion: 0,
		State: clusterpb.ClusterTopology_STABLE,
	}
	if err := m.storage.CreateClusterTopology(ctx, clusterTopologyPb); err != nil {
		return nil, errors.Wrapf(err, "cluster manager CreateCluster, clusterTopology:%v", clusterTopologyPb)
	}

	cluster := NewCluster(clusterPb, m.storage)
	if err := cluster.Load(ctx); err != nil {
		return nil, errors.Wrapf(err, "cluster manager CreateCluster, clusterName:%s", clusterName)
	}

	m.cluster[clusterName] = cluster
	return cluster, nil
}

func (m *ManagerImpl) AllocSchemaID(ctx context.Context, clusterName, schemaName string) (uint32, error) {
	cluster, err := m.getCluster(ctx, clusterName)
	if err != nil {
		return 0, errors.Wrap(err, "cluster manager AllocSchemaID")
	}

	schema, exists := cluster.getSchema(schemaName)
	if exists {
		return schema.GetID(), nil
	}
	// create new schema
	schemaID, err := m.allocSchemaID(clusterName)
	if err != nil {
		return 0, errors.Wrapf(err, "cluster manager AllocSchemaID, "+
			"clusterName:%s, schemaName:%s", clusterName, schemaName)
	}
	if _, err1 := cluster.CreateSchema(ctx, schemaName, schemaID); err1 != nil {
		return 0, errors.Wrapf(err, "cluster manager AllocSchemaID, "+
			"clusterName:%s, schemaName:%s", clusterName, schemaName)
	}
	return schemaID, nil
}

func (m *ManagerImpl) AllocTableID(ctx context.Context, clusterName, schemaName, tableName, node string) (uint64, error) {
	cluster, err := m.getCluster(ctx, clusterName)
	if err != nil {
		return 0, errors.Wrap(err, "cluster manager AllocTableID")
	}

	table, exists, err := cluster.getTable(ctx, schemaName, tableName)
	if err != nil {
		return 0, errors.Wrapf(err, "cluster manager AllocTableID, "+
			"clusterName:%s, schemaName:%s, tableName:%s, node:%s", clusterName, schemaName, tableName, node)
	}
	if exists {
		return table.getID(), nil
	}
	// create new schemas
	tableID, err := m.allocTableID(clusterName)
	if err != nil {
		return 0, errors.Wrapf(err, "cluster manager AllocTableID, "+
			"clusterName:%s, schemaName:%s, tableName:%s, node:%s", clusterName, schemaName, tableName, node)
	}
	shardID, err := m.allocShardID(clusterName, node)
	if err != nil {
		return 0, errors.Wrapf(err, "cluster manager AllocTableID, "+
			"clusterName:%s, schemaName:%s, tableName:%s, node:%s", clusterName, schemaName, tableName, node)
	}

	if _, err := cluster.CreateTable(ctx, schemaName, shardID, tableName, tableID); err != nil {
		return 0, errors.Wrapf(err, "cluster manager AllocTableID, "+
			"clusterName:%s, schemaName:%s, tableName:%s, node:%s", clusterName, schemaName, tableName, node)
	}
	return tableID, nil
}

func (m *ManagerImpl) GetTables(ctx context.Context, clusterName, node string, shardIDs []uint32) (map[uint32]*ShardTables, error) {
	cluster, err := m.getCluster(ctx, clusterName)
	if err != nil {
		return nil, errors.Wrap(err, "cluster manager GetTables")
	}

	shardTablesWithRole, err := cluster.GetTables(ctx, shardIDs, node)
	if err != nil {
		return nil, errors.Wrapf(err, "cluster manager GetTables, "+
			"clusterName:%s, node:%s, shardIDs:%v", clusterName, node, shardIDs)
	}

	ret := make(map[uint32]*ShardTables, len(shardIDs))
	for shardID, shardTables := range shardTablesWithRole {
		tableInfos := make([]*TableInfo, len(shardTables.tables))

		for _, t := range shardTables.tables {
			tableInfos = append(tableInfos, &TableInfo{
				id: t.meta.GetId(), name: t.meta.GetName(),
				schemaID: t.schema.GetId(), schemaName: t.schema.GetName(),
			})
		}
		ret[shardID] = &ShardTables{shardRole: shardTables.shardRole, tables: tableInfos, version: shardTables.version}
	}
	return ret, nil
}

func (m *ManagerImpl) DropTable(ctx context.Context, clusterName, schemaName, tableName string, tableID uint64) error {
	cluster, err := m.getCluster(ctx, clusterName)
	if err != nil {
		return errors.Wrap(err, "cluster manager DropTable")
	}

	if err := cluster.DropTable(ctx, schemaName, tableName, tableID); err != nil {
		return errors.Wrapf(err, "cluster manager DropTable, "+
			"clusterName:%s, schemaName:%s, tableName:%s, tableID:%d", clusterName, schemaName, tableName, tableID)
	}

	return nil
}

func (m *ManagerImpl) RegisterNode(ctx context.Context, clusterName, node string, lease uint32) error {
	nodePb := &clusterpb.Node{NodeStats: &clusterpb.NodeStats{Lease: lease}, Name: node}
	cluster, err := m.getCluster(ctx, clusterName)
	if err != nil {
		return errors.Wrap(err, "cluster manager RegisterNode")
	}
	nodePb1, err := m.storage.CreateOrUpdateNode(ctx, cluster.clusterID, nodePb)
	if err != nil {
		return errors.Wrapf(err, "cluster manager RegisterNode, clusterName:%s, node:%s", clusterName, node)
	}
	cluster.Lock()
	cluster.metaData.nodeMap[node] = nodePb1
	cluster.Unlock()

	// todo: refactor coordinator
	if err := cluster.coordinator.Run(ctx); err != nil {
		return errors.Wrap(err, "RegisterNode")
	}
	return nil
}

func (m *ManagerImpl) GetShards(ctx context.Context, clusterName, node string) ([]uint32, error) {
	cluster, err := m.getCluster(ctx, clusterName)
	if err != nil {
		return nil, errors.Wrap(err, "cluster manager GetShards")
	}
	shardIDs, ok := cluster.nodes[node]
	if !ok {
		return nil, ErrNodeNotFound.WithCausef("cluster manager GetShards, "+
			"clusterName:%s, node:%s", clusterName, node)
	}
	return shardIDs, nil
}

func (m *ManagerImpl) getCluster(ctx context.Context, clusterName string) (*Cluster, error) {
	cluster, ok := m.cluster[clusterName]
	if !ok {
		return nil, ErrClusterNotFound.WithCausef("cluster manager getCluster, clusterName:%s", clusterName)
	}
	return cluster, nil
}

func (m *ManagerImpl) allocClusterID() (uint32, error) {
	return 0, nil
}

func (m *ManagerImpl) allocSchemaID(clusterName string) (uint32, error) {
	return 0, nil
}

func (m *ManagerImpl) allocTableID(clusterName string) (uint64, error) {
	return 0, nil
}

func (m *ManagerImpl) allocShardID(clusterName, node string) (uint32, error) {
	return 0, nil
}
