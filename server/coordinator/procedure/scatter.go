// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/schedule"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
)

const (
	EventScatterPrepare = "EventScatterPrepare"
	EventScatterFailed  = "EventScatterFailed"
	EventScatterSuccess = "EventScatterSuccess"

	StateScatterBegin   = "StateScatterBegin"
	StateScatterWaiting = "StateScatterWaiting"
	StateScatterFinish  = "StateScatterFinish"
	StateScatterFailed  = "StateScatterFailed"
)

var (
	ScatterEvents = fsm.Events{
		{Name: EventScatterPrepare, Src: []string{StateScatterBegin}, Dst: StateScatterWaiting},
		{Name: EventTransferLeaderSuccess, Src: []string{StateScatterWaiting}, Dst: StateScatterFinish},
		{Name: EventScatterFailed, Src: []string{StateScatterBegin, StateScatterWaiting}, Dst: StateScatterFailed},
	}
	ScatterCallbacks = fsm.Callbacks{
		EventScatterPrepare: func(event *fsm.Event) {
			// FIXME: The following logic is used for static topology, which is a bit simple and violent.
			// It needs to be modified when supporting dynamic topology.
			request := event.Args[0].(*ScatterCallbackRequest)
			c := request.c
			handler := request.handler
			nodeInfo := request.nodeInfo
			ctx := request.cxt

			if c.GetClusterState() == clusterpb.ClusterTopology_STABLE {
				shardIDs, err := c.GetShardIDs(nodeInfo.GetEndpoint())
				if err != nil {
					event.Cancel(errors.WithMessage(err, "procedure scatterShard"))
				}
				if len(nodeInfo.GetShardInfos()) == 0 {
					if err := handler.Dispatch(ctx, nodeInfo.GetEndpoint(), &schedule.OpenEvent{ShardIDs: shardIDs}); err != nil {
						event.Cancel(errors.WithMessage(err, "procedure scatterShard"))
					}
				}
			}

			if !(int(c.GetClusterMinNodeCount()) <= len(c.GetNodesCache()) &&
				c.GetClusterState() == clusterpb.ClusterTopology_EMPTY) {
				event.Cancel()
			}

			shardTotal := int(c.GetClusterShardTotal())
			minNodeCount := int(c.GetClusterMinNodeCount())
			perNodeShardCount := shardTotal / minNodeCount
			shards := make([]*clusterpb.Shard, 0, shardTotal)
			nodeList := make([]*clusterpb.Node, 0, len(c.GetNodesCache()))
			for _, v := range c.GetNodesCache() {
				nodeList = append(nodeList, v.GetMeta())
			}

			for i := 0; i < minNodeCount; i++ {
				for j := 0; j < perNodeShardCount; j++ {
					if i*perNodeShardCount+j < shardTotal {
						// TODO: consider nodesCache state
						shards = append(shards, &clusterpb.Shard{
							Id:        uint32(i*perNodeShardCount + j),
							ShardRole: clusterpb.ShardRole_LEADER,
							Node:      nodeList[i].GetName(),
						})
					}
				}
			}

			if err := c.UpdateClusterTopology(ctx, clusterpb.ClusterTopology_STABLE, shards); err != nil {
				event.Cancel(errors.WithMessage(err, "procedure scatterShard"))
			}

			if err := c.Load(ctx); err != nil {
				event.Cancel(errors.WithMessage(err, "procedure scatterShard"))
			}

			for nodeName, node := range c.GetNodesCache() {
				if err := handler.Dispatch(ctx, nodeName, &schedule.OpenEvent{ShardIDs: node.GetShardIDs()}); err != nil {
					event.Cancel(errors.WithMessage(err, "procedure scatterShard"))
				}
			}
		},
		EventScatterFailed: func(event *fsm.Event) {
			// nolint
		},
		EventScatterSuccess: func(event *fsm.Event) {
			// nolint
		},
	}
)

type ScatterProcedure struct {
	id    uint64
	state State

	fsm      *fsm.FSM
	c        *cluster.Cluster
	nodeInfo *metaservicepb.NodeInfo
	handler  *schedule.EventHandler
}

func NewScatterProcedure(cluster *cluster.Cluster, nodeInfo *metaservicepb.NodeInfo) *ScatterProcedure {
	ScatterOperationFsm := fsm.NewFSM(
		StateScatterBegin,
		ScatterEvents,
		ScatterCallbacks,
	)

	// TODO: fix id alloc
	id := uint64(1)
	return &ScatterProcedure{fsm: ScatterOperationFsm, id: id, state: StateInit, c: cluster, nodeInfo: nodeInfo}
}

type ScatterCallbackRequest struct {
	p       *ScatterProcedure
	c       *cluster.Cluster
	handler *schedule.EventHandler
	cxt     context.Context

	nodeInfo *metaservicepb.NodeInfo
}

func (p *ScatterProcedure) ID() uint64 {
	return p.id
}

func (p *ScatterProcedure) Type() Type {
	return TransferLeader
}

func (p *ScatterProcedure) Start(ctx context.Context) error {
	p.state = StateRunning

	request := ScatterCallbackRequest{
		p:        p,
		c:        p.c,
		handler:  p.handler,
		cxt:      ctx,
		nodeInfo: p.nodeInfo,
	}
	if err := p.fsm.Event(EventScatterPrepare); err != nil {
		p.state = StateFailed
		if err := p.fsm.Event(EventScatterFailed, request); err != nil {
			// TODO: EventTransferLeaderFailed event failed, how to process rollback invalid?
			return errors.WithMessage(err, "coordinator scatter shard")
		}
		return errors.WithMessage(err, "coordinator scatter shard")
	}

	if err := p.fsm.Event(EventScatterPrepare, request); err != nil {
		return errors.WithMessage(err, "coordinator scatter shard")
	}
	p.state = StateFinished
	return nil
}

func (p *ScatterProcedure) Cancel(ctx context.Context) error {
	p.state = StateCancelled
	return nil
}

func (p *ScatterProcedure) State() State {
	return p.state
}
