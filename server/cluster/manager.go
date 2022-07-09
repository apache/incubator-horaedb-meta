package cluster

import (
	"context"

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
	Load(ctx context.Context, storage storage.Storage) error
	AllocSchemaID(ctx context.Context, clusterName, schemaName string) (uint32, error)
	AllocTableID(ctx context.Context, clusterName, tableName string) (uint64, error)
	GetTables(ctx context.Context, clusterName string, shardIDs []uint32) (map[uint32]ShardTables, error)
	DropTable(ctx context.Context, clusterName, schemaName, tableName string, tableID uint64) error
}

type ManagerImpl struct {
	clusterIDs []uint32
	cluster    map[string]*Cluster
}

func NewManagerImpl(clusterIDs []uint32) *ManagerImpl {
	return &ManagerImpl{clusterIDs: clusterIDs}
}

func (m *ManagerImpl) Load(ctx context.Context, storage storage.Storage) error {
	m.cluster = make(map[string]*Cluster, len(m.clusterIDs))
	for _, id := range m.clusterIDs {
		cluster := NewCluster(id, storage)
		cluster.Load(ctx)
		m.cluster[cluster.Name()] = cluster
	}
	return nil
}

func (m *ManagerImpl) AllocSchemaID(ctx context.Context, schemaName string) (uint32, error) {
	return 0, nil
}

func (m *ManagerImpl) AllocTableID(ctx context.Context, tableName string) (uint64, error) {
	return 0, nil
}

func (m *ManagerImpl) GetTables(ctx context.Context, clusterName, node string, shardIDs []uint32) (map[uint32]*ShardTables, error) {
	cluster, ok := m.cluster[clusterName]
	if !ok {
		return nil, ErrClusterNotFound.WithCausef("cluster_name", clusterName)
	}

	shardTablesWithRole, err := cluster.GetTables(ctx, shardIDs, node)
	if err != nil {
		return nil, errors.Wrap(err, "get_tables")
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
	return nil
}
