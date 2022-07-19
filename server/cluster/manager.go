package cluster

import (
	"context"
	"sync"
	"time"

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
	AllocTableID(ctx context.Context, node, clusterName, schemaName, tableName string) (uint64, error)
	GetTables(ctx context.Context, node, clusterName string, shardIDs []uint32) (map[uint32]*ShardTables, error)
	DropTable(ctx context.Context, clusterName, schemaName, tableName string, tableID uint64) error
	RegisterNode(ctx context.Context, clusterName, node string, lease uint32) error
	GetShards(ctx context.Context, clusterName, node string) ([]uint32, error)
}

type ManagerImpl struct {
	sync.RWMutex
	clusterIDs []uint32
	cluster    map[string]*Cluster
	storage    storage.Storage
}

func NewManagerImpl(storage storage.Storage) *ManagerImpl {
	return &ManagerImpl{storage: storage, cluster: make(map[string]*Cluster, 0)}
}

func (m *ManagerImpl) Load(ctx context.Context) error {
	// todo load clusterIDs
	m.cluster = make(map[string]*Cluster, len(m.clusterIDs))
	for _, id := range m.clusterIDs {
		cluster := NewCluster(id, m.storage)
		if err := cluster.Load(ctx); err != nil {
			return errors.Wrap(err, "Load")
		}
		m.cluster[cluster.Name()] = cluster
	}
	return nil
}

func (m *ManagerImpl) CreateCluster(ctx context.Context, clusterName string, nodeCount, replicationFactor, shardTotal uint32) (*Cluster, error) {
	m.Lock()
	defer m.Unlock()
	_, ok := m.cluster[clusterName]
	if ok {
		return nil, ErrClusterAlreadyExists
	}

	clusterID, err := m.allocClusterID()
	if err != nil {
		return nil, errors.Wrap(err, "CreateCluster")
	}

	clusterPb := &clusterpb.Cluster{Id: clusterID, Name: clusterName, MinNodeCount: nodeCount, ReplicationFactor: replicationFactor, ShardTotal: shardTotal}
	if err := m.storage.CreateCluster(ctx, clusterPb); err != nil {
		return nil, errors.Wrap(err, "CreateCluster")
	}

	// todo: add schedule
	clusterTopologyPb := &clusterpb.ClusterTopology{ClusterId: clusterID, DataVersion: 0, State: clusterpb.ClusterTopology_STABLE, CreatedAt: uint64(time.Now().UnixMilli())}
	if err := m.storage.CreateClusterTopology(ctx, clusterTopologyPb); err != nil {
		return nil, errors.Wrap(err, "CreateCluster")
	}

	cluster := NewCluster(clusterID, m.storage)
	if err := cluster.Load(ctx); err != nil {
		return nil, errors.Wrap(err, "CreateCluster")
	}

	m.cluster[clusterName] = cluster
	return cluster, nil
}

func (m *ManagerImpl) AllocSchemaID(ctx context.Context, clusterName, schemaName string) (uint32, error) {
	cluster, err := m.getCluster(ctx, clusterName)
	if err != nil {
		return 0, errors.Wrap(err, "AllocSchemaID")
	}

	schema, exists := cluster.getSchema(schemaName)
	if exists {
		return schema.GetID(), nil

	}
	// create new schemas
	schemaID, err := m.allocSchemaID(clusterName)
	if err != nil {
		return 0, errors.Wrap(err, "AllocSchemaID")
	}
	if _, err1 := cluster.CreateSchema(ctx, schemaName, schemaID); err1 != nil {
		return 0, errors.Wrap(err, "AllocSchemaID")
	}
	return schemaID, nil

}

func (m *ManagerImpl) AllocTableID(ctx context.Context, node, clusterName, schemaName, tableName string) (uint64, error) {
	cluster, err := m.getCluster(ctx, clusterName)
	if err != nil {
		return 0, errors.Wrap(err, "AllocTableID")
	}

	table, exists, err := cluster.getTable(ctx, schemaName, tableName)
	if err != nil {
		return 0, errors.Wrap(err, "AllocSchemaID")
	}
	if exists {
		return table.getID(), nil
	}
	// create new schemas
	tableID, err := m.allocTableID(clusterName)
	if err != nil {
		return 0, errors.Wrapf(err, "AllocTableID")
	}
	shardID, err := m.allocShardID(clusterName, node)
	if err != nil {
		return 0, errors.Wrapf(err, "AllocTableID")
	}

	if _, err := cluster.CreateTable(ctx, schemaName, shardID, tableName, tableID); err != nil {
		return 0, errors.Wrap(err, "AllocSchemaID")
	}
	return tableID, nil
}

func (m *ManagerImpl) GetTables(ctx context.Context, node, clusterName string, shardIDs []uint32) (map[uint32]*ShardTables, error) {
	cluster, err := m.getCluster(ctx, clusterName)
	if err != nil {
		return nil, errors.Wrap(err, "GetTables")
	}

	shardTablesWithRole, err := cluster.GetTables(ctx, shardIDs, node)
	if err != nil {
		return nil, errors.Wrap(err, "GetTables")
	}

	ret := make(map[uint32]*ShardTables, len(shardIDs))
	for shardID, shardTables := range shardTablesWithRole {
		tableInfos := make([]*TableInfo, len(shardTables.tables))

		for _, t := range shardTables.tables {
			tableInfos = append(tableInfos, &TableInfo{id: t.meta.GetId(), name: t.meta.GetName(), schemaID: t.schema.GetId(), schemaName: t.schema.GetName()})
		}
		ret[shardID] = &ShardTables{shardRole: shardTables.shardRole, tables: tableInfos, version: shardTables.version}
	}
	return ret, nil
}

func (m *ManagerImpl) DropTable(ctx context.Context, clusterName, schemaName, tableName string, tableID uint64) error {
	cluster, err := m.getCluster(ctx, clusterName)
	if err != nil {
		return errors.Wrap(err, "DropTable")
	}

	if err := cluster.DropTable(ctx, schemaName, tableName, tableID); err != nil {
		return errors.Wrap(err, "DropTable")
	}

	return nil
}

func (m *ManagerImpl) RegisterNode(ctx context.Context, clusterName, node string, lease uint32) error {
	nodePb := &clusterpb.Node{NodeStats: &clusterpb.NodeStats{Lease: lease}, Name: node}
	cluster, err := m.getCluster(ctx, clusterName)
	if err != nil {
		return errors.Wrap(err, "RegisterNode")
	}
	nodePb1, err := m.storage.CreateOrUpdateNode(ctx, cluster.clusterID, nodePb)
	if err != nil {
		return errors.Wrap(err, "RegisterNode")
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
		return nil, errors.Wrapf(err, "GetShards, cluster_name:%s", clusterName)
	}
	shardIDs, ok := cluster.nodes[node]
	if !ok {
		return nil, ErrNodeNotFound.WithCausef("GetShards, cluster_name:%s, node:%s", clusterName, node)
	}
	return shardIDs, nil
}

func (m *ManagerImpl) getCluster(ctx context.Context, clusterName string) (*Cluster, error) {
	cluster, ok := m.cluster[clusterName]
	if !ok {
		return nil, ErrClusterNotFound.WithCausef("getCluster", clusterName)
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
