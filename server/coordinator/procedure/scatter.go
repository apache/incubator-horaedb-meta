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
			request := event.Args[0].(*ScatterCallbackRequest)
			// FIXME: The following logic is used for static topology, which is a bit simple and violent.
			// It needs to be modified when supporting dynamic topology.
			c := request.c
			handler := request.handler
			nodeInfo := request.nodeInfo
			ctx := request.cxt

			if c.GetMetaData().GetClusterTopology().State == clusterpb.ClusterTopology_STABLE {
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

			if !(int(c.GetMetaData().GetCluster().MinNodeCount) <= len(c.GetNodesCache()) &&
				c.GetMetaData().GetClusterTopology().State == clusterpb.ClusterTopology_EMPTY) {
				event.Cancel()
			}

			shardTotal := int(c.GetMetaData().GetCluster().ShardTotal)
			minNodeCount := int(c.GetMetaData().GetCluster().MinNodeCount)
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

			c.GetMetaData().GetClusterTopology().ShardView = shards
			c.GetMetaData().GetClusterTopology().State = clusterpb.ClusterTopology_STABLE
			if err := c.GetStorage().PutClusterTopology(ctx, c.GetClusterID(), c.GetMetaData().GetClusterTopology().Version, c.GetMetaData().GetClusterTopology()); err != nil {
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

		},
		EventScatterSuccess: func(event *fsm.Event) {

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

type ScatterCallbackRequest struct {
	p       *TransferLeaderProcedure
	c       *cluster.Cluster
	handler *schedule.EventHandler
	cxt     context.Context

	leaderFsm   *fsm.FSM
	followerFsm *fsm.FSM
	nodeInfo    *metaservicepb.NodeInfo
}

func (p *ScatterProcedure) ID() uint64 {
	return p.id
}

func (p *ScatterProcedure) Type() Type {
	return TransferLeader
}

func (p *ScatterProcedure) Start(ctx context.Context) error {
	p.state = StateRunning

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
