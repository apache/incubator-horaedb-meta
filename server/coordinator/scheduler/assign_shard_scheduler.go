// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package scheduler

import (
	"context"

	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/storage"
)

const (
	AssignReason = "ShardView exists in metadata but shardNode not exists, assign shard to node"
)

// AssignShardScheduler used to assigning shards without nodes.
type AssignShardScheduler struct {
	factory    coordinator.Factory
	nodePicker coordinator.NodePicker
}

func NewAssignShardScheduler(factory coordinator.Factory, nodePicker coordinator.NodePicker) Scheduler {
	return &AssignShardScheduler{
		factory:    factory,
		nodePicker: nodePicker,
	}
}

func (a AssignShardScheduler) Schedule(ctx context.Context, clusterView storage.ClusterView, shardViews []storage.ShardView) (Result, error) {
	if clusterView.State != storage.ClusterStateStable {
		return Result{}, cluster.ErrClusterStateInvalid
	}

	// Check if there is a shard without node mapping.
	for _, shardView := range shardViews {
		exists, _ := contains(shardView.ShardID, clusterView.ShardNodes)
		if exists {
			continue
		}
		newLeaderNode, err := a.nodePicker.PickNode(ctx, clusterView.ClusterName)
		if err != nil {
			return Result{}, err
		}
		// Shard exists and ShardNode not exists.
		p, err := a.factory.CreateTransferLeaderProcedure(ctx, coordinator.TransferLeaderRequest{
			ClusterName:       clusterView.ClusterName,
			ShardID:           shardView.ShardID,
			OldLeaderNodeName: "",
			NewLeaderNodeName: newLeaderNode.Node.Name,
			ClusterVersion:    clusterView.Version,
		})
		if err != nil {
			return Result{}, err
		}
		return Result{
			p:               p,
			schedulerReason: AssignReason,
		}, nil
	}
	return Result{}, nil
}

func contains(shardID storage.ShardID, shardNodes []storage.ShardNode) (bool, storage.ShardNode) {
	for _, shardNode := range shardNodes {
		if shardID == shardNode.ID {
			return true, shardNode
		}
	}
	return false, storage.ShardNode{}
}
