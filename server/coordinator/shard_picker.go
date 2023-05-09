// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator

import (
	"context"
	"crypto/rand"
	"math/big"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
)

// ShardPicker is used to pick up the shards suitable for scheduling in the cluster.
// If expectShardNum bigger than cluster node number, the result depends on enableDuplicateNode:
// TODO: Consider refactor this interface, abstracts the parameters of PickShards as PickStrategy.
type ShardPicker interface {
	PickShards(ctx context.Context, snapshot metadata.Snapshot, expectShardNum int) ([]storage.ShardNode, error)
}

// RandomBalancedShardPicker randomly pick up shards that are not on the same node in the current cluster.
type RandomBalancedShardPicker struct{}

func NewRandomBalancedShardPicker() ShardPicker {
	return &RandomBalancedShardPicker{}
}

// PickShards will pick a specified number of shards as expectShardNum.
func (p *RandomBalancedShardPicker) PickShards(_ context.Context, snapshot metadata.Snapshot, expectShardNum int) ([]storage.ShardNode, error) {
	shardNodes := snapshot.Topology.ClusterView.ShardNodes

	nodeShardsMapping := make(map[string][]storage.ShardNode, 0)
	for _, shardNode := range shardNodes {
		_, exists := nodeShardsMapping[shardNode.NodeName]
		if !exists {
			nodeShardsMapping[shardNode.NodeName] = []storage.ShardNode{}
		}
		nodeShardsMapping[shardNode.NodeName] = append(nodeShardsMapping[shardNode.NodeName], shardNode)
	}

	// Try to make shards on different nodes.
	result := make([]storage.ShardNode, 0, expectShardNum)
	totalShardLength := len(shardNodes)
	tempNodeShardMapping := make(map[string][]storage.ShardNode, len(nodeShardsMapping))
	nodeNames := make([]string, 0, len(nodeShardsMapping))

	for i := 0; i < expectShardNum; i++ {
		// Initialize nodeNames.
		if len(nodeNames) == 0 {
			for nodeName := range nodeShardsMapping {
				nodeNames = append(nodeNames, nodeName)
			}
		}

		// Initialize tempNodeShardMapping in following cases:
		// 1. First time to pick shards.
		// 2. All shards are already picked.
		if len(result) == 0 || len(result) == totalShardLength {
			for nodeName, shardNode := range nodeShardsMapping {
				tempShardNode := make([]storage.ShardNode, len(shardNode))
				copy(tempShardNode, shardNode)
				tempNodeShardMapping[nodeName] = tempShardNode
			}
		}

		// Get random node.
		selectNodeIndex, err := rand.Int(rand.Reader, big.NewInt(int64(len(nodeNames))))
		if err != nil {
			return nil, errors.WithMessage(err, "generate random node index")
		}
		nodeShards := tempNodeShardMapping[nodeNames[selectNodeIndex.Int64()]]

		// Get random shard.
		selectNodeShardsIndex, err := rand.Int(rand.Reader, big.NewInt(int64(len(nodeShards))))
		if err != nil {
			return nil, errors.WithMessage(err, "generate random node shard index")
		}

		if len(nodeShards) > 0 {
			result = append(result, nodeShards[selectNodeShardsIndex.Int64()])

			// Remove select shard.
			nodeShards[selectNodeShardsIndex.Int64()] = nodeShards[len(nodeShards)-1]
			tempNodeShardMapping[nodeNames[selectNodeIndex.Int64()]] = nodeShards[:len(nodeShards)-1]
		}

		// Remove select node.
		nodeNames[selectNodeIndex.Int64()] = nodeNames[len(nodeNames)-1]
		nodeNames = nodeNames[:len(nodeNames)-1]
	}

	return result, nil
}
