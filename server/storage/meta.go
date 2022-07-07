// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import (
	"context"

	"github.com/CeresDB/ceresdbproto/pkg/metapb"
)

// MetaStorage defines the storage operations on the ceresdb cluster meta info.
type MetaStorage interface {
	GetCluster(clusterId uint32, meta *metapb.Cluster) (bool, error)
	PutCluster(clusterId uint32, meta *metapb.Cluster) error

	GetClusterTopology(clusterId uint32, clusterMetaData *metapb.ClusterTopology) (bool, error)
	PutClusterTopology(clusterId uint32, clusterMetaData *metapb.ClusterTopology) error

	GetSchemas(ctx context.Context, clusterId uint32, schemas []*metapb.Schema) error
	PutSchemas(ctx context.Context, clusterId uint32, schemas []*metapb.Schema) error

	GetTables(clusterId uint32, schemaId uint32, tableId []uint64, table []*metapb.Table) (bool, error)
	PutTables(clusterId uint32, schemaId uint32, tables []*metapb.Table) error
	DeleteTables(clusterId uint32, schemaId uint32, tableIds []uint64) (bool, error)

	GetShardTopologies(clusterId uint32, shardId []uint32, shardTableInfo []*metapb.ShardTopology) (bool, error)
	PutShardTopologies(clusterId uint32, shardId []uint32, shardTableInfo []*metapb.ShardTopology) error

	GetNodes(clusterId uint32, node []*metapb.Node) (bool, error)
	PutNodes(clusterId uint32, node []*metapb.Node) error
}
