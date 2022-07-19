package cluster

import "github.com/CeresDB/ceresdbproto/pkg/clusterpb"

type Shard struct {
	meta    []*clusterpb.Shard
	nodes   []*clusterpb.Node
	tables  []*Table
	version uint64
}

type ShardTablesWithRole struct {
	shardRole ShardRole
	tables    []*Table
	version   uint64
}
