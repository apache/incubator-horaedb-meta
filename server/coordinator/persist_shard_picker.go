package coordinator

import (
	"context"
	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/storage"
	"github.com/pkg/errors"
)

type PersistShardPicker struct {
	cluster  *metadata.ClusterMetadata
	internal ShardPicker
}

func NewPersistShardPicker(cluster *metadata.ClusterMetadata, internal ShardPicker) *PersistShardPicker {
	return &PersistShardPicker{cluster: cluster, internal: internal}
}

func (p *PersistShardPicker) PickShards(ctx context.Context, snapshot metadata.Snapshot, schemaName string, tableNames []string) (map[string]storage.ShardNode, error) {
	result := map[string]storage.ShardNode{}

	shardNodeMap := map[storage.ShardID]storage.ShardNode{}
	for _, shardNode := range snapshot.Topology.ClusterView.ShardNodes {
		shardNodeMap[shardNode.ID] = shardNode
	}

	// If table assign has been created, just reuse it.
	for i := 0; i < len(tableNames); i++ {
		shardID, exists, err := p.cluster.GetAssignTable(ctx, schemaName, tableNames[i])
		if err != nil {
			return map[string]storage.ShardNode{}, err
		}
		if exists {
			result[tableNames[i]] = shardNodeMap[shardID]
		}
	}

	if len(result) == len(tableNames) {
		return result, nil
	}

	if len(result) != len(tableNames) && len(result) != 0 {
		// TODO: Should all table assigns be cleared?
		return result, errors.WithMessagef(ErrPickNode, "The number of table assign %d is inconsistent with the number of tables %d", len(result), len(tableNames))
	}

	// No table assign has been created, try to pick shard and save table assigns.
	shardNodes, err := p.internal.PickShards(ctx, snapshot, len(tableNames))
	if err != nil {
		return map[string]storage.ShardNode{}, err
	}

	for i, shardNode := range shardNodes {
		result[tableNames[i]] = shardNode
		err = p.cluster.AssignTable(ctx, schemaName, tableNames[i], shardNode.ID)
		if err != nil {
			return map[string]storage.ShardNode{}, err
		}
	}

	return result, nil
}
