// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator

import (
	"context"
	"crypto/rand"
	"math/big"
	"sort"

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
	if len(snapshot.Topology.ClusterView.ShardNodes) == 0 {
		return nil, errors.WithMessage(ErrNodeNumberNotEnough, "no shard is assigned")
	}

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
	nodeNames := make([]string, 0, len(nodeShardsMapping))
	tempNodeShardMapping := copyNodeShardMapping(nodeShardsMapping)

	for i := 0; i < expectShardNum; i++ {
		// Initialize nodeNames.
		if len(nodeNames) == 0 {
			for nodeName := range nodeShardsMapping {
				nodeNames = append(nodeNames, nodeName)
			}
		}

		// Get random node.
		selectNodeIndex, err := rand.Int(rand.Reader, big.NewInt(int64(len(nodeNames))))
		if err != nil {
			return nil, errors.WithMessage(err, "generate random node index")
		}
		nodeShards := tempNodeShardMapping[nodeNames[selectNodeIndex.Int64()]]

		// When node shards is empty, copy from nodeShardsMapping and get shards again.
		if len(nodeShards) == 0 {
			tempNodeShardMapping = copyNodeShardMapping(nodeShardsMapping)

			nodeShards = tempNodeShardMapping[nodeNames[selectNodeIndex.Int64()]]
		}

		// Get random shard.
		selectNodeShardsIndex, err := rand.Int(rand.Reader, big.NewInt(int64(len(nodeShards))))
		if err != nil {
			return nil, errors.WithMessage(err, "generate random node shard index")
		}

		result = append(result, nodeShards[selectNodeShardsIndex.Int64()])

		// Remove select shard.
		nodeShards[selectNodeShardsIndex.Int64()] = nodeShards[len(nodeShards)-1]
		tempNodeShardMapping[nodeNames[selectNodeIndex.Int64()]] = nodeShards[:len(nodeShards)-1]

		// Remove select node.
		nodeNames[selectNodeIndex.Int64()] = nodeNames[len(nodeNames)-1]
		nodeNames = nodeNames[:len(nodeNames)-1]
	}

	return result, nil
}

func copyNodeShardMapping(nodeShardsMapping map[string][]storage.ShardNode) map[string][]storage.ShardNode {
	tempNodeShardMapping := make(map[string][]storage.ShardNode, len(nodeShardsMapping))
	for nodeName, shardNode := range nodeShardsMapping {
		tempShardNode := make([]storage.ShardNode, len(shardNode))
		copy(tempShardNode, shardNode)
		tempNodeShardMapping[nodeName] = tempShardNode
	}
	return tempNodeShardMapping
}

// LeastTableShardPicker selects shards based on the number of tables on the current shard,
// and always selects the shard with the smallest number of current tables.
type LeastTableShardPicker struct{}

func NewLeastTableShardPicker() ShardPicker {
	return &LeastTableShardPicker{}
}

func (l LeastTableShardPicker) PickShards(_ context.Context, snapshot metadata.Snapshot, expectShardNum int) ([]storage.ShardNode, error) {
	shardNodeMapping := make(map[storage.ShardID]storage.ShardNode, len(snapshot.Topology.ShardViewsMapping))
	for _, shardNode := range snapshot.Topology.ClusterView.ShardNodes {
		shardNodeMapping[shardNode.ID] = shardNode
	}

	if len(snapshot.Topology.ClusterView.ShardNodes) == 0 {
		return nil, errors.WithMessage(ErrNodeNumberNotEnough, "no shard is assigned")
	}

	result := make([]storage.ShardNode, 0, expectShardNum)

	shardTableNumberMapping := make(map[storage.ShardID]int, len(snapshot.Topology.ShardViewsMapping))
	for shardID, shardView := range snapshot.Topology.ShardViewsMapping {
		shardTableNumberMapping[shardID] = len(shardView.TableIDs)
	}

	var sortedMapping []Pair
	for i := 0; i < expectShardNum; i++ {
		if i%len(shardTableNumberMapping) == 0 {
			sortedMapping = sortByTableNumber(shardTableNumberMapping)
		}

		selectShard := sortedMapping[i%len(shardTableNumberMapping)]
		shardTableNumberMapping[selectShard.shardID]++

		result = append(result, shardNodeMapping[selectShard.shardID])
	}

	return result, nil
}

type Pair struct {
	shardID     storage.ShardID
	tableNumber int
}

// sortByTableNumber sort shard by table number,
// the shard with the smallest number of tables is at the front of the array.
func sortByTableNumber(shardTableNumberMapping map[storage.ShardID]int) []Pair {
	vec := make([]Pair, 0, len(shardTableNumberMapping))
	for shardID, tableNumber := range shardTableNumberMapping {
		vec = append(vec, Pair{shardID: shardID, tableNumber: tableNumber})
	}

	sort.Slice(vec, func(i, j int) bool {
		return vec[i].tableNumber < vec[j].tableNumber
	})

	return vec
}
