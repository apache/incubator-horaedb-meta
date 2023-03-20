package scheduler

import (
	"context"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/storage"
)

// AssignShardScheduler used to assigning shards without nodes.
type AssignShardScheduler struct {
	clusterName string
	factory     coordinator.Factory
	nodePicker  coordinator.NodePicker
}

func (a AssignShardScheduler) Schedule(ctx context.Context, clusterView storage.ClusterView, shardViews []storage.ShardView) (error, procedure.Procedure, string) {
	if clusterView.State != storage.ClusterStateStable {
		return nil, nil, ""
	}

	// Check if there is a shard without node mapping.
	for _, shardView := range shardViews {
		exists, _ := contains(shardView.ShardID, clusterView.ShardNodes)
		if exists {
			continue
		}
		newLeaderNode, err := a.nodePicker.PickNode(ctx, a.clusterName)
		if err != nil {
			return err, nil, ""
		}
		// Shard exists and ShardNode not exists.
		p, err := a.factory.CreateTransferLeaderProcedure(ctx, coordinator.TransferLeaderRequest{
			ClusterName:       a.clusterName,
			ShardID:           shardView.ShardID,
			OldLeaderNodeName: "",
			NewLeaderNodeName: newLeaderNode.Node.Name,
			ClusterVersion:    clusterView.Version,
		})
		if err != nil {
			return err, p, ""
		}
		return nil, p, ""
	}
	return nil, nil, ""
}

func contains(shardID storage.ShardID, shardNodes []storage.ShardNode) (bool, storage.ShardNode) {
	for _, shardNode := range shardNodes {
		if shardID == shardNode.ID {
			return true, shardNode
		}
	}
	return false, storage.ShardNode{}
}
