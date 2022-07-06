// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.
// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// fork from: https://github.com/tikv/pd/blob/master/server/storage/endpoint/meta.go

package storage

import (
	"github.com/CeresDB/ceresdbproto/pkg/metapb"
)

// MetaStorage defines the storage operations on the ceresdb cluster meta info.
type MetaStorage interface {
	GetCluster(clusterId uint32, meta *metapb.Cluster) (bool, error)
	PutCluster(clusterId uint32, meta *metapb.Cluster) error

	GetClusterTopology(clusterId uint32, clusterMetaData *metapb.ClusterTopology) (bool, error)
	PutClusterTopology(clusterId uint32, clusterMetaData *metapb.ClusterTopology) error

	GetSchemas(clusterId uint32, schemas []*metapb.Schema) (bool, error)
	PutSchemas(clusterId uint32, schemas []*metapb.Schema) error

	GetTables(clusterId uint32, schemaId uint32, tableId []uint64, table []*metapb.Table) (bool, error)
	PutTables(clusterId uint32, schemaId uint32, tables []*metapb.Table) error
	DeleteTables(clusterId uint32, schemaId uint32, tableIDs []uint64) (bool, error)

	GetShardTopologies(clusterId uint32, shardId []uint32, shardTableInfo []*metapb.ShardTopology) (bool, error)
	PutShardTopologies(clusterId uint32, shardId []uint32, shardTableInfo []*metapb.ShardTopology) error

	GetNodes(clusterId uint32, node []*metapb.Node) (bool, error)
	PutNodes(clusterId uint32, node []*metapb.Node) error
}
