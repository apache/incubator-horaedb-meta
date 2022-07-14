package cluster

import "github.com/CeresDB/ceresdbproto/pkg/metapb"

type Shard struct {
	meta    []*metapb.Shard
	nodes   []*metapb.Node
	tables  []*Table
	version uint64
}

type ShardTablesWithRole struct {
	shardRole ShardRole
	tables    []*Table
	version   uint64
}
