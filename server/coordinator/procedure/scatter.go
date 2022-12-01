// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"sync"
	"time"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	eventScatterPrepare = "EventScatterPrepare"
	eventScatterFailed  = "EventScatterFailed"
	eventScatterSuccess = "EventScatterSuccess"

	stateScatterBegin   = "StateScatterBegin"
	stateScatterWaiting = "StateScatterWaiting"
	stateScatterFinish  = "StateScatterFinish"
	stateScatterFailed  = "StateScatterFailed"

	defaultCheckNodeNumTimeInterval = time.Second * 3
)

var (
	scatterEvents = fsm.Events{
		{Name: eventScatterPrepare, Src: []string{stateScatterBegin}, Dst: stateScatterWaiting},
		{Name: eventScatterSuccess, Src: []string{stateScatterWaiting}, Dst: stateScatterFinish},
		{Name: eventScatterFailed, Src: []string{stateScatterWaiting}, Dst: stateScatterFailed},
	}
	scatterCallbacks = fsm.Callbacks{
		eventScatterPrepare: scatterPrepareCallback,
		eventScatterFailed:  scatterFailedCallback,
		eventScatterSuccess: scatterSuccessCallback,
	}
)

func scatterPrepareCallback(event *fsm.Event) {
	request, err := getRequestFromEvent[*ScatterCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}

	c := request.cluster
	ctx := request.ctx

	waitForNodesReady(c)

	if c.GetClusterState() == storage.ClusterStateStable {
		return
	}

	registeredNodes := c.GetRegisteredNodes()
	shardTotal := c.GetTotalShardNum()
	minNodeCount := c.GetClusterMinNodeCount()

	shardNodes, err := allocNodeShards(shardTotal, minNodeCount, registeredNodes, request.shardIDs)
	if err != nil {
		cancelEventWithLog(event, err, "alloc node shardNodes failed")
		return
	}

	shardViews := make([]cluster.CreateShardView, 0, shardTotal)
	for _, shard := range shardNodes {
		shardViews = append(shardViews, cluster.CreateShardView{
			ShardID: shard.ID,
			Tables:  []storage.TableID{},
		})
	}

	if err := c.UpdateClusterView(ctx, storage.ClusterStateStable, shardNodes); err != nil {
		cancelEventWithLog(event, err, "update cluster view")
		return
	}

	if err := c.CreateShardViews(ctx, shardViews); err != nil {
		cancelEventWithLog(event, err, "create shard views")
		return
	}

	for _, shard := range shardNodes {
		openShardRequest := eventdispatch.OpenShardRequest{
			Shard: cluster.ShardInfo{
				ID:      shard.ID,
				Role:    storage.ShardRoleLeader,
				Version: 0,
			},
		}
		if err := request.dispatch.OpenShard(ctx, shard.NodeName, openShardRequest); err != nil {
			cancelEventWithLog(event, err, "open shard failed")
			return
		}
	}
}

func waitForNodesReady(c *cluster.Cluster) {
	for {
		time.Sleep(defaultCheckNodeNumTimeInterval)

		nodes := c.GetRegisteredNodes()

		currNodeNum := computeOnlineNodeNum(nodes)
		expectNodeNum := c.GetClusterMinNodeCount()
		log.Warn("wait for cluster node register", zap.Uint32("currNodeNum", currNodeNum), zap.Uint32("expectNodeNum", expectNodeNum))

		if currNodeNum < expectNodeNum {
			continue
		}
		break
	}
}

// Compute the total number of the available nodes.
func computeOnlineNodeNum(nodes []cluster.RegisteredNode) uint32 {
	onlineNodeNum := uint32(0)
	for _, node := range nodes {
		if node.IsOnline() {
			onlineNodeNum++
		}
	}
	return onlineNodeNum
}

// Allocates shard ids across the registered nodes, and caller should ensure `minNodeCount <= len(allNodes)`.
func allocNodeShards(shardTotal uint32, minNodeCount uint32, allNodes []cluster.RegisteredNode, shardIDs []storage.ShardID) ([]storage.ShardNode, error) {
	// If the number of registered nodes exceeds the required number of nodes, intercept the first registered nodes.
	if len(allNodes) > int(minNodeCount) {
		allNodes = allNodes[:minNodeCount]
	}

	shards := make([]storage.ShardNode, 0, shardTotal)

	perNodeShardCount := shardTotal / minNodeCount
	if shardTotal%minNodeCount != 0 {
		perNodeShardCount++
	}

	for i := uint32(0); i < minNodeCount; i++ {
		for j := uint32(0); j < perNodeShardCount; j++ {
			if uint32(len(shards)) < shardTotal {
				shardID := shardIDs[len(shards)]
				// TODO: consider nodesCache state
				shards = append(shards, storage.ShardNode{
					ID:        shardID,
					ShardRole: storage.ShardRoleLeader,
					NodeName:  allNodes[i].Node.Name,
				})
			}
		}
	}

	return shards, nil
}

func scatterSuccessCallback(_ *fsm.Event) {
	log.Info("scatter procedure execute finish")
}

func scatterFailedCallback(_ *fsm.Event) {
	// TODO: Use RollbackProcedure to rollback transfer failed
}

// ScatterCallbackRequest is fsm callbacks param.
type ScatterCallbackRequest struct {
	cluster  *cluster.Cluster
	ctx      context.Context
	dispatch eventdispatch.Dispatch
	shardIDs []storage.ShardID
}

func NewScatterProcedure(dispatch eventdispatch.Dispatch, cluster *cluster.Cluster, id uint64, shardIDs []storage.ShardID) Procedure {
	scatterProcedureFsm := fsm.NewFSM(
		stateScatterBegin,
		scatterEvents,
		scatterCallbacks,
	)

	return &ScatterProcedure{id: id, state: StateInit, fsm: scatterProcedureFsm, dispatch: dispatch, cluster: cluster, shardIDs: shardIDs}
}

type ScatterProcedure struct {
	id       uint64
	fsm      *fsm.FSM
	dispatch eventdispatch.Dispatch
	cluster  *cluster.Cluster
	shardIDs []storage.ShardID

	// Protect the state.
	lock  sync.RWMutex
	state State
}

func (p *ScatterProcedure) ID() uint64 {
	return p.id
}

func (p *ScatterProcedure) Typ() Typ {
	return Scatter
}

func (p *ScatterProcedure) Start(ctx context.Context) error {
	log.Info("scatter procedure start", zap.Uint64("procedureID", p.id))

	p.updateStateWithLock(StateRunning)

	scatterCallbackRequest := &ScatterCallbackRequest{
		cluster:  p.cluster,
		ctx:      ctx,
		dispatch: p.dispatch,
		shardIDs: p.shardIDs,
	}

	if err := p.fsm.Event(eventScatterPrepare, scatterCallbackRequest); err != nil {
		err1 := p.fsm.Event(eventScatterFailed, scatterCallbackRequest)
		p.updateStateWithLock(StateFailed)
		if err1 != nil {
			err = errors.WithMessagef(err, "scatter procedure start, fail to send eventScatterFailed err:%v", err1)
		}
		return errors.WithMessage(err, "scatter procedure start")
	}

	if err := p.fsm.Event(eventScatterSuccess, scatterCallbackRequest); err != nil {
		return errors.WithMessage(err, "scatter procedure start")
	}

	p.updateStateWithLock(StateFinished)
	return nil
}

func (p *ScatterProcedure) Cancel(_ context.Context) error {
	p.updateStateWithLock(StateCancelled)
	return nil
}

func (p *ScatterProcedure) State() State {
	return p.state
}

func (p *ScatterProcedure) updateStateWithLock(state State) {
	p.lock.Lock()
	p.state = state
	p.lock.Unlock()
}
