// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package scheduler

import (
	"context"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator"
)

type RebalancedShardScheduler struct {
	factory    *coordinator.Factory
	nodePicker coordinator.NodePicker
}

func NewRebalancedShardScheduler(factory *coordinator.Factory, nodePicker coordinator.NodePicker) Scheduler {
	return &RebalancedShardScheduler{
		factory:    factory,
		nodePicker: nodePicker,
	}
}

func (r RebalancedShardScheduler) Schedule(ctx context.Context, clusterSnapshot metadata.Snapshot) (ScheduleResult, error) {
	// RebalancedShardScheduler can only be scheduled when the cluster is stable.
	if !clusterSnapshot.Topology.IsStable() {
		return ScheduleResult{}, nil
	}

	// TODO: Improve scheduling efficiency and verify whether the topology changes.
	for _, shardNode := range clusterSnapshot.Topology.ClusterView.ShardNodes {
		node, err := r.nodePicker.PickNode(ctx, shardNode.ID, clusterSnapshot.RegisteredNodes)
		if err != nil {
			return ScheduleResult{}, err
		}
		if node.Node.Name != shardNode.NodeName {
			p, err := r.factory.CreateTransferLeaderProcedure(ctx, coordinator.TransferLeaderRequest{
				Snapshot:          clusterSnapshot,
				ShardID:           shardNode.ID,
				OldLeaderNodeName: shardNode.NodeName,
				NewLeaderNodeName: node.Node.Name,
			})
			if err != nil {
				return ScheduleResult{}, err
			}
			return ScheduleResult{
				Procedure: p,
				Reason:    "",
			}, nil
		}
	}

	return ScheduleResult{}, nil
}
