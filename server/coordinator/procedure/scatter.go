// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"sync"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/dispatch"
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
	scatterEvents = fsm.Events{
		{Name: EventScatterPrepare, Src: []string{StateScatterBegin}, Dst: StateScatterWaiting},
		{Name: EventScatterSuccess, Src: []string{StateScatterWaiting}, Dst: StateScatterFinish},
		{Name: EventScatterFailed, Src: []string{StateScatterWaiting}, Dst: StateScatterFailed},
	}
	scatterCallbacks = fsm.Callbacks{
		EventScatterPrepare: scatterPrepareCallback,
		EventScatterFailed:  scatterFailedCallback,
		EventScatterSuccess: scatterSuccessCallback,
	}
)

func scatterPrepareCallback(event *fsm.Event) {
	request := event.Args[0].(*ScatterCallbackRequest)
	requestCluster := request.cluster
	requestDispatch := request.dispatch
	ctx := request.cxt
	nodeInfo := request.nodeInfo

	if requestCluster.GetClusterState() == clusterpb.ClusterTopology_STABLE {
		shardIDs, err := requestCluster.GetShardIDs(nodeInfo.GetEndpoint())
		if err != nil {
			event.Cancel(errors.WithMessage(err, "coordinator scatterShard"))
			return
		}
		if len(nodeInfo.GetShardInfos()) == 0 {
			if err := requestDispatch.OpenShards(ctx, nodeInfo.GetEndpoint(), dispatch.OpenShardAction{ShardIDs: shardIDs}); err != nil {
				event.Cancel(errors.WithMessage(err, "coordinator scatterShard"))
				return
			}
		}
	}

	nodeCache := requestCluster.GetClusterNodeCache()
	shardTotal := requestCluster.GetClusterShardTotal()
	minNodeCount := requestCluster.GetClusterMinNodeCount()

	if !(int(minNodeCount) <= len(nodeCache) &&
		requestCluster.GetClusterState() == clusterpb.ClusterTopology_EMPTY) {
		event.Cancel()
		return
	}

	perNodeShardCount := shardTotal / minNodeCount

	shards := make([]*clusterpb.Shard, 0, shardTotal)
	nodeList := make([]*clusterpb.Node, 0, len(nodeCache))
	for _, v := range nodeCache {
		nodeList = append(nodeList, v.GetMeta())
	}

	for i := uint32(0); i < minNodeCount; i++ {
		for j := uint32(0); j < perNodeShardCount; j++ {
			shardID := i*perNodeShardCount + j
			if shardID < shardTotal {
				// requestCluster.LockShardByID(shardID)
				// defer requestCluster.UnlockShardByID(shardID)
				// TODO: consider nodesCache state
				shards = append(shards, &clusterpb.Shard{
					Id:        shardID,
					ShardRole: clusterpb.ShardRole_LEADER,
					Node:      nodeList[i].GetName(),
				})
			}
		}
	}

	for nodeName, node := range nodeCache {
		if err := requestDispatch.OpenShards(ctx, nodeName, dispatch.OpenShardAction{ShardIDs: node.GetShardIDs()}); err != nil {
			event.Cancel(errors.WithMessage(err, "coordinator scatterShard"))
			return
		}
	}

	if err := requestCluster.UpdateClusterTopology(ctx, clusterpb.ClusterTopology_STABLE, shards); err != nil {
		event.Cancel(errors.WithMessage(err, "coordinator scatterShard"))
		return
	}
}

func scatterSuccessCallback(event *fsm.Event) {
	request := event.Args[0].(*ScatterCallbackRequest)
	requestCluster := request.cluster
	ctx := request.cxt

	if err := requestCluster.Load(ctx); err != nil {
		event.Cancel(errors.WithMessage(err, "coordinator scatterShard"))
		return
	}
}

func scatterFailedCallback(_ *fsm.Event) {
	// TODO: Use RollbackProcedure to rollback transfer failed
}

// ScatterCallbackRequest is fsm callbacks param
type ScatterCallbackRequest struct {
	cluster  *cluster.Cluster
	cxt      context.Context
	dispatch dispatch.ActionDispatch

	nodeInfo *metaservicepb.NodeInfo
}

func NewScatterProcedure(dispatch dispatch.ActionDispatch, cluster *cluster.Cluster, nodeInfo *metaservicepb.NodeInfo) *ScatterProcedure {
	scatterProcedureFsm := fsm.NewFSM(
		StateScatterBegin,
		scatterEvents,
		scatterCallbacks,
	)
	// TODO: Alloc ID by ID Allocator
	id := uint64(1)

	return &ScatterProcedure{id: id, state: StateInit, fsm: scatterProcedureFsm, dispatch: dispatch, cluster: cluster, nodeInfo: nodeInfo}
}

type ScatterProcedure struct {
	lock     sync.RWMutex
	id       uint64
	state    State
	fsm      *fsm.FSM
	dispatch dispatch.ActionDispatch

	cluster  *cluster.Cluster
	nodeInfo *metaservicepb.NodeInfo
}

func (p *ScatterProcedure) ID() uint64 {
	return p.id
}

func (p *ScatterProcedure) Typ() Typ {
	return Scatter
}

func (p *ScatterProcedure) Start(ctx context.Context) error {
	p.UpdateStateWithLock(StateRunning)

	scatterCallbackRequest := &ScatterCallbackRequest{
		cluster:  p.cluster,
		cxt:      ctx,
		dispatch: p.dispatch,
		nodeInfo: p.nodeInfo,
	}

	if err := p.fsm.Event(EventScatterPrepare, scatterCallbackRequest); err != nil {
		err := p.fsm.Event(EventScatterFailed, scatterCallbackRequest)
		p.UpdateStateWithLock(StateFailed)
		return errors.WithMessage(err, "coordinator transferLeaderShard start")
	}

	if err := p.fsm.Event(EventScatterSuccess, scatterCallbackRequest); err != nil {
		return errors.WithMessage(err, "coordinator transferLeaderShard start")
	}

	p.UpdateStateWithLock(StateFinished)
	return nil
}

func (p *ScatterProcedure) Cancel(_ context.Context) error {
	p.UpdateStateWithLock(StateCancelled)
	return nil
}

func (p *ScatterProcedure) State() State {
	return p.state
}

func (p *ScatterProcedure) UpdateStateWithLock(state State) {
	p.lock.Lock()
	p.state = state
	p.lock.Unlock()
}
