// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import (
	"fmt"
	"path"
)

const (
	cluster       = "v1/cluster"
	schema        = "schema"
	node          = "node"
	table         = "table"
	shard         = "shard"
	topology      = "topo"
	latestVersion = "latest_version"
)

// makeSchemaKey returns the schema meta info key path with the given region ID.
// example:
// cluster 1: v1/cluster/1/schema/1 -> ceresmeta.Schema
//            v1/cluster/1/schema/2 -> ceresmeta.Schema
//            v1/cluster/1/schema/3 -> ceresmeta.Schema
func makeSchemaKey(clusterID uint32, schemaID uint32) string {
	return path.Join(cluster, fmtID(uint64(clusterID)), schema, fmtID(uint64(schemaID)))
}

// v1/cluster/1/node/1 -> ceresmeta.Node
func makeNodeKey(clusterID uint32, nodeID uint32) string {
	return path.Join(cluster, fmtID(uint64(clusterID)), node, fmtID(uint64(nodeID)))
}

// v1/cluster/1 -> ceresmeta.Cluster
func makeClusterKey(clusterID uint32) string {
	return path.Join(cluster, fmtID(uint64(clusterID)))
}

// v1/cluster/1/schema/1/table/1 -> ceresmeta.Table
func makeTableKey(clusterID uint32, schemaID uint32, tableID uint64) string {
	return path.Join(cluster, fmtID(uint64(clusterID)), schema, fmtID(uint64(schemaID)), table, fmtID(tableID))
}

// v1/cluster/1/shard/1/1 -> ceresmeta.Shard
func makeShardKey(clusterID uint32, shardID uint32, latestVersion string) string {
	return path.Join(cluster, fmtID(uint64(clusterID)), shard, fmtID(uint64(shardID)), latestVersion)
}

// v1/cluster/1/topo/1 -> ceresmeta.ClusterTopology
func makeClusterTopologyKey(clusterID uint32, latestVersion string) string {
	return path.Join(cluster, fmtID(uint64(clusterID)), topology, latestVersion)
}

// v1/cluster/1/topo/latestVersion -> ceresmeta.ClusterTopologyLatestVersion
func makeClusterTopologyLatestVersionKey(clusterID uint32) string {
	return path.Join(cluster, fmtID(uint64(clusterID)), topology, latestVersion)
}

// v1/cluster/1/shard/1/latestVersion -> ceresmeta.ShardLatestVersion
func makeShardLatestVersionKey(clusterID uint32, shardID uint32) string {
	return path.Join(cluster, fmtID(uint64(clusterID)), shard, fmtID(uint64(shardID)), latestVersion)
}

func fmtID(id uint64) string {
	return fmt.Sprintf("%020d", id)
}
