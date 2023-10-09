// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator

import (
	"context"
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

// LeastTableShardPicker selects shards based on the number of tables on the current shard,
// and always selects the shard with the smallest number of current tables.
type leastTableShardPicker struct{}

func NewLeastTableShardPicker() ShardPicker {
	return &leastTableShardPicker{}
}

func (l leastTableShardPicker) PickShards(_ context.Context, snapshot metadata.Snapshot, expectShardNum int) ([]storage.ShardNode, error) {
	shardNodeMapping := make(map[storage.ShardID]storage.ShardNode, len(snapshot.Topology.ShardViewsMapping))
	for _, shardNode := range snapshot.Topology.ClusterView.ShardNodes {
		shardNodeMapping[shardNode.ID] = shardNode
	}

	if len(snapshot.Topology.ClusterView.ShardNodes) == 0 {
		return nil, errors.WithMessage(ErrNodeNumberNotEnough, "no shard is assigned")
	}

	result := make([]storage.ShardNode, 0, expectShardNum)

	ShardIDs := make([]storage.ShardID, 0, len(snapshot.Topology.ShardViewsMapping))
	for shardID := range snapshot.Topology.ShardViewsMapping {
		ShardIDs = append(ShardIDs, shardID)
	}

	// sort shard by table number,
	// the shard with the smallest number of tables is at the front of the array.
	sort.SliceStable(ShardIDs, func(i, j int) bool {
		return len(snapshot.Topology.ShardViewsMapping[ShardIDs[i]].TableIDs) < len(snapshot.Topology.ShardViewsMapping[ShardIDs[j]].TableIDs)
	})

	for i := 0; i < expectShardNum; i++ {
		selectShardID := ShardIDs[i%len(ShardIDs)]
		result = append(result, shardNodeMapping[selectShardID])
	}

	return result, nil
}
