// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package data

import (
	"sync"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/server/storage"
)

// nolint
type Topology struct {
	storage storage.Storage

	// RWMutex is used to protect following fields.
	lock        sync.RWMutex
	clusterMeta *clusterpb.Cluster // schema meta info in storage
	// ClusterTopology in memory.
	shardNodesMapping map[uint32][]*clusterpb.Shard // shardID -> nodes of the shard
	nodeShardsMapping map[string][]*clusterpb.Shard // nodeName -> shards of the node
	version           uint64                        // clusterTopology version
	// ShardTopology in memory.
	shardTablesMapping map[uint32]*clusterpb.ShardTopology // shardID -> shardTopology
	tableShardMapping  map[uint32]uint32                   // tableID -> shardID
}
