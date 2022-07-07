// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import (
	"context"

	"github.com/CeresDB/ceresdbproto/pkg/metapb"
)

// MetaStorage defines the storage operations on the ceresdb cluster meta info.
type MetaStorage interface {
	GetCluster(clusterID uint32, meta *metapb.Cluster) (bool, error)
	PutCluster(clusterID uint32, meta *metapb.Cluster) error

	GetClusterTopology(clusterID uint32, clusterMetaData *metapb.ClusterTopology) (bool, error)
	PutClusterTopology(clusterID uint32, clusterMetaData *metapb.ClusterTopology) error

	GetSchemas(ctx context.Context, clusterID uint32, schemas []*metapb.Schema) error
	PutSchemas(ctx context.Context, clusterID uint32, schemas []*metapb.Schema) error

	GetTables(clusterID uint32, schemaID uint32, tableID []uint64, table []*metapb.Table) (bool, error)
	PutTables(clusterID uint32, schemaID uint32, tables []*metapb.Table) error
	DeleteTables(clusterID uint32, schemaID uint32, tableIDs []uint64) (bool, error)

	GetShardTopologies(clusterID uint32, shardID []uint32, shardTableInfo []*metapb.ShardTopology) (bool, error)
	PutShardTopologies(clusterID uint32, shardID []uint32, shardTableInfo []*metapb.ShardTopology) error

	GetNodes(clusterID uint32, node []*metapb.Node) (bool, error)
	PutNodes(clusterID uint32, node []*metapb.Node) error
}
