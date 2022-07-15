// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import (
	"fmt"
	"path"
)

const (
	cluster = "v1/cluster"
	schema  = "schema"
	node    = "node"
	table   = "table"
	shard   = "shard"
	topo    = "topo"
	lv      = "latest_version"
)

// makeSchemaKey returns the schema meta info key path with the given region ID.
// example:
// cluster 1: v1/cluster/1/schema/1 -> ceresmeta.Schema
//            v1/cluster/1/schema/2 -> ceresmeta.Schema
//            v1/cluster/1/schema/3 -> ceresmeta.Schema
func makeSchemaKey(clusterID uint32, schemaID uint32) string {
	return path.Join(cluster, fmt.Sprintf("%020d", clusterID), schema, fmt.Sprintf("%020d", schemaID))
}

func makeNodeKey(clusterID uint32, nodeID uint32) string {
	return path.Join(cluster, fmt.Sprintf("%020d", clusterID), node, fmt.Sprintf("%020d", nodeID))
}

func makeClusterKey(clusterID uint32) string {
	return path.Join(cluster, fmt.Sprintf("%020d", clusterID))
}

func makeTableKey(clusterID uint32, schemaID uint32, tableID uint64) string {
	return path.Join(cluster, fmt.Sprintf("%020d", clusterID), schema, fmt.Sprintf("%020d", schemaID), table, fmt.Sprintf("%020d", tableID))
}

func makeShardKey(clusterID uint32, shardID uint32, latestVersion string) string {
	return path.Join(cluster, fmt.Sprintf("%020d", clusterID), shard, fmt.Sprintf("%020d", shardID), latestVersion)
}

func makeClusterTopologyKey(clusterID uint32, latestVersion string) string {
	return path.Join(cluster, fmt.Sprintf("%020d", clusterID), topo, latestVersion)
}

func makeLatestVersion(clusterID uint32) string {
	return path.Join(cluster, fmt.Sprintf("%020d", clusterID), topo, lv)
}

func makeShardLatestVersion(clusterID uint32, shardID uint32) string {
	return path.Join(cluster, fmt.Sprintf("%020d", clusterID), shard, fmt.Sprintf("%020d", shardID), lv)
}
