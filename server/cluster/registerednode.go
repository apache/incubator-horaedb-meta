// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"github.com/CeresDB/ceresmeta/server/storage"
)

type RegisteredNode struct {
	node       storage.Node
	shardInfos []ShardInfo
}

func NewRegisteredNode(meta storage.Node, shardInfos []ShardInfo) RegisteredNode {
	return RegisteredNode{
		meta,
		shardInfos,
	}
}

func (n *RegisteredNode) GetShardInfos() []ShardInfo {
	return n.shardInfos
}

func (n *RegisteredNode) GetNode() storage.Node {
	return n.node
}

func (n *RegisteredNode) IsOnline() bool {
	return n.node.State == storage.Online
}

func (n RegisteredNode) IsExpired(now uint64, aliveThreshold uint64) bool {
	return now >= aliveThreshold+n.GetNode().LastTouchTime
}
