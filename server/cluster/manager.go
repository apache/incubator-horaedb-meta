package cluster

import (
	"context"

	"github.com/CeresDB/ceresmeta/pkg/coderr"
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

func (m *ManagerImpl) AllocSchemaID(ctx context.Context, clusterName, schemaName string) (uint32, error) {
	cluster, err := m.getCluster(ctx, clusterName)
	if err != nil {
		return 0, errors.Wrap(err, "AllocSchemaID")
	}

	schema, err := cluster.getSchema(ctx, schemaName)
	if err != nil {
		if coderr.Is(err, coderr.NotFound) {
			// create new schema
			schemaID := m.allocSchemaID()
			if _, err1 := cluster.CreateSchema(ctx, schemaName, schemaID); err1 != nil {
				return 0, errors.Wrap(err, "AllocSchemaID")
			}
			// add to cache

			return schemaID, nil
		}
		return 0, errors.Wrap(err, "AllocSchemaID")
	}

	return schema.ID(), nil
}

func (m *ManagerImpl) AllocTableID(ctx context.Context, clusterName, schemaName, tableName string) (uint64, error) {
	return 0, nil
}

func (m *ManagerImpl) GetTables(ctx context.Context, clusterName, node string, shardIDs []uint32) (map[uint32]*ShardTables, error) {
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
	return nil
}

func (m *ManagerImpl) getCluster(ctx context.Context, clusterName string) (*Cluster, error) {
	cluster, ok := m.cluster[clusterName]
	if !ok {
		return nil, ErrClusterNotFound.WithCausef("getCluster", clusterName)
	}
	return cluster, nil
}

func (m *ManagerImpl) allocSchemaID() uint32 {
	return 0
}
