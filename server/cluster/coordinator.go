// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"context"
	"sync"
	"time"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/schedule"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// TODO: heartbeatInterval should be set in config
const DefaultHeartbeatInterval = time.Second * 3

// coordinator is used to decide if the shards need to be scheduled.
type coordinator struct {
	// Mutex is to ensure only one coordinator can run at the same time.
	lock            sync.Mutex
	cluster         *Cluster
	clusterBalancer *clusterBalancer

	ctx          context.Context
	cancel       context.CancelFunc
	bgJobWg      *sync.WaitGroup
	eventHandler *schedule.EventHandler
}

func newCoordinator(cluster *Cluster, hbstreams *schedule.HeartbeatStreams) *coordinator {
	ctx, cancel := context.WithCancel(context.Background())
	return &coordinator{
		cluster: cluster,

		ctx:          ctx,
		cancel:       cancel,
		bgJobWg:      &sync.WaitGroup{},
		eventHandler: schedule.NewEventHandler(hbstreams),
	}
}

func (c *coordinator) stop() {
	c.cancel()
	c.bgJobWg.Wait()
}

func (c *coordinator) runBgJob() {
	c.bgJobWg.Add(1)
	defer c.bgJobWg.Done()

	log.Info("coordinator runBgJob started", zap.String("cluster", c.cluster.Name()))
	for {
		t := time.After(DefaultHeartbeatInterval)
		select {
		case <-t:
			for nodeName := range c.cluster.nodesCache {
				err := c.eventHandler.Dispatch(c.ctx, nodeName, &schedule.NoneEvent{})
				if err != nil {
					log.Error("fail to send node event msg", zap.Error(err), zap.String("node", nodeName))
				}
			}

		case <-c.ctx.Done():
			log.Warn("exit from background jobs")
			return
		}
	}
}

func (c *coordinator) Close() {
	c.cancel()
	c.bgJobWg.Wait()
}

// TODO: consider ReplicationFactor
func (c *coordinator) scatterShard(ctx context.Context, nodeInfo *metaservicepb.NodeInfo) error {
	// FIXME: The following logic is used for static topology, which is a bit simple and violent.
	// It needs to be modified when supporting dynamic topology.
	if c.cluster.metaData.clusterTopology.State == clusterpb.ClusterTopology_STABLE {
		shardIDs, err := c.cluster.GetShardIDs(nodeInfo.GetEndpoint())
		if err != nil {
			return errors.WithMessage(err, "coordinator scatterShard")
		}
		if len(nodeInfo.GetShardInfos()) == 0 {
			if err := c.eventHandler.Dispatch(ctx, nodeInfo.GetEndpoint(), &schedule.OpenEvent{ShardIDs: shardIDs}); err != nil {
				return errors.WithMessage(err, "coordinator scatterShard")
			}
		}
	}

	if !(int(c.cluster.metaData.cluster.MinNodeCount) <= len(c.cluster.nodesCache) &&
		c.cluster.metaData.clusterTopology.State == clusterpb.ClusterTopology_EMPTY) {
		return nil
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	shardTotal := int(c.cluster.metaData.cluster.ShardTotal)
	minNodeCount := int(c.cluster.metaData.cluster.MinNodeCount)
	perNodeShardCount := shardTotal / minNodeCount
	shards := make([]*clusterpb.Shard, 0, shardTotal)
	nodeList := make([]*clusterpb.Node, 0, len(c.cluster.nodesCache))
	for _, v := range c.cluster.nodesCache {
		nodeList = append(nodeList, v.meta)
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

	c.cluster.metaData.clusterTopology.ShardView = shards
	c.cluster.metaData.clusterTopology.State = clusterpb.ClusterTopology_STABLE
	if err := c.cluster.storage.PutClusterTopology(ctx, c.cluster.clusterID, c.cluster.metaData.clusterTopology.Version, c.cluster.metaData.clusterTopology); err != nil {
		return errors.WithMessage(err, "coordinator scatterShard")
	}

	if err := c.cluster.Load(ctx); err != nil {
		return errors.WithMessage(err, "coordinator scatterShard")
	}

	for nodeName, node := range c.cluster.nodesCache {
		if err := c.eventHandler.Dispatch(ctx, nodeName, &schedule.OpenEvent{ShardIDs: node.shardIDs}); err != nil {
			return errors.WithMessage(err, "coordinator scatterShard")
		}
	}
	return nil
}

/*
*
Only Leader Shard(`Replication_Factor` == 1)
 1. Same as `Multi Follower Shard` leader shard.

Multi Follower Shard(`Replication_Factor` >= 2)
- Leader Shard：
 1. First, CeresMeta adjusts the routing relationship and create a new follower shard on the node.
 2. Follower shard notifies CeresMeta after initialization.
 3. CeresMeta started the process of leader switch, switched the leader to the newly created follower shard.
 4. Close the original leader shard after the leader switch is completed.

- Follower Shard：
 1. Adjust shard node mapping to directly assign this shard to another node.
*/
func (c *coordinator) migrateShard(shard *clusterpb.Shard, targetNode *Node) error {
	// create new follower shard, reduce unavailable time
	shardId, err := c.cluster.allocShardID(c.ctx)
	if err != nil {
		return errors.WithMessage(err, "coordinator migrateShard")
	}

	clusterTopology := c.cluster.metaData.clusterTopology
	newFollowerShard := &clusterpb.Shard{
		Id:        shardId,
		ShardRole: clusterpb.ShardRole_FOLLOWER,
		Node:      targetNode.meta.Name,
	}
	clusterTopology.ShardView = append(clusterTopology.ShardView, newFollowerShard)

	// Update tables on new Shard
	shardTopologies, err := c.cluster.storage.ListShardTopologies(c.ctx, c.cluster.clusterID, []uint32{shard.GetId()})
	for i := 0; i < len(shardTopologies); i++ {
		if shardTopologies[i].ShardId == shard.Id {
			tables := shardTopologies[i].GetTableIds()
			shardTopology := &clusterpb.ShardTopology{
				ShardId:  shardId,
				TableIds: tables,
				Version:  0,
			}

			if err := c.cluster.storage.PutShardTopology(c.ctx, c.cluster.clusterID, shardTopology.GetVersion(), shardTopology); err != nil {
				return errors.WithMessage(err, "coordinator migrateShard")
			}
		}
	}

	if err := c.cluster.storage.PutClusterTopology(c.ctx, c.cluster.clusterID, c.cluster.metaData.clusterTopology.Version, c.cluster.metaData.clusterTopology); err != nil {
		return errors.WithMessage(err, "coordinator migrateShard")
	}

	if err := c.cluster.Load(c.ctx); err != nil {
		return errors.WithMessage(err, "coordinator migrateShard")
	}

	// New follower shard create finish, migrate shard by transfer leader
	if err := c.transferLeaderShard(c.ctx, shard, newFollowerShard); err != nil {
		return errors.WithMessage(err, "coordinator migrateShard")
	}

	// Migrate shard finish, delete origin leader shard
	if err := c.eventHandler.Dispatch(c.ctx, targetNode.meta.GetName(), &schedule.CloseEvent{ShardIDs: []uint32{shard.Id}}); err != nil {
		return errors.WithMessage(err, "coordinator scatterShard")
	}
	for i := 0; i < len(clusterTopology.ShardView); i++ {
		if clusterTopology.ShardView[i] == shard {
			clusterTopology.ShardView = append(clusterTopology.ShardView[:i], clusterTopology.ShardView[i+1:]...)
		}
	}
	return nil
}

/*
*
Only Leader Shard(`Replication_Factor` == 1)
 1. When there is only one leader shard, there is no need to switch leader, CeresMeta only need to migrate shard to another node.

Multi Follower Shard(`Replication_Factor` >= 2)
 1. First, set the original leader shard and the follower to be switched to PENDING_FOLLOWER and PENDING_LEADER.
 2. Wait for the leader shard to complete the preparatory action before the leader switch, and notify the CeresMeta after the completion. In this process, the new leader cannot process the write request.
 3. After receiving this request, CeresMeta will update the status of the two shards, and the leader switch is completed.
*/
func (c *coordinator) transferLeaderShard(ctx context.Context, oldLeader *clusterpb.Shard, newLeader *clusterpb.Shard) error {
	// When `Replication_Factor` == 1, newLeader is nil, find a suitable node and migrate shard to this node
	if newLeader == nil {
		targetNode := c.clusterBalancer.selectNode()
		return c.migrateShard(oldLeader, targetNode)
	}

	leaderFsm := NewFSM(clusterpb.ShardRole_LEADER)
	followerFsm := NewFSM(clusterpb.ShardRole_FOLLOWER)

	if err := leaderFsm.Event(EventPrepareTransferFollower); err != nil {
		return errors.WithMessage(err, "coordinator transferLeaderShard")
	}
	if err := followerFsm.Event(EventPrepareTransferLeader); err != nil {
		return errors.WithMessage(err, "coordinator transferLeaderShard")
	}

	// Leader transfer first, follower wait until leader transfer finish
	if err := leaderFsm.Event(EventTransferFollower); err != nil {
		leaderFsm.Event(EventTransferFollowerFailed)
		return errors.WithMessage(err, "coordinator transferLeaderShard")
	}
	if err := followerFsm.Event(EventTransferLeader); err != nil {
		followerFsm.Event(EventTransferLeaderFailed)
		return errors.WithMessage(err, "coordinator transferLeaderShard")
	}

	// Update cluster topology
	currentTopology := c.cluster.metaData.clusterTopology
	for i := 0; i < len(currentTopology.ShardView); i++ {
		shardId := currentTopology.ShardView[i].Id
		if shardId == oldLeader.Id {
			currentTopology.ShardView[i].ShardRole = clusterpb.ShardRole_FOLLOWER
		}
		if shardId == newLeader.Id {
			currentTopology.ShardView[i].ShardRole = clusterpb.ShardRole_LEADER
		}
	}
	if err := c.cluster.storage.PutClusterTopology(ctx, c.cluster.clusterID, c.cluster.metaData.clusterTopology.Version, c.cluster.metaData.clusterTopology); err != nil {
		return errors.WithMessage(err, "coordinator scatterShard")
	}

	if err := c.cluster.Load(ctx); err != nil {
		return errors.WithMessage(err, "coordinator scatterShard")
	}
	return nil
}

// Just supported split to 2 shards, if target split size >2, repeat shard split
func (c *coordinator) splitShard(shard *clusterpb.Shard, targetNode *Node) error {
	shardTablesWithRole, err := c.cluster.GetTables(c.ctx, []uint32{shard.Id}, shard.Node)
	if err != nil {
		return errors.WithMessage(err, "coordinator splitShard")
	}

	tables := shardTablesWithRole[shard.Id].tables
	if len(tables) <= 1 {
		return ErrSplitShard.WithCausef("the number of tables in the shard is less than 2, cannot be split, shardID:%d", shard.Id)
	}

	// TODO: Consider better split strategy
	originNodeTables := tables[0 : len(tables)/2]
	targetNodeTables := tables[len(tables)/2+1 : len(tables)]

	originNodeTablesID := make([]uint64, 0, len(originNodeTables))
	for i := 0; i < len(originNodeTables); i++ {
		originNodeTablesID = append(originNodeTablesID, originNodeTables[i].meta.Id)
	}
	targetNodeTablesID := make([]uint64, 0, len(targetNodeTables))
	for i := 0; i < len(targetNodeTables); i++ {
		targetNodeTablesID = append(targetNodeTablesID, targetNodeTables[i].meta.Id)
	}

	// Create new shard on same node
	newShardId, err := c.cluster.allocShardID(c.ctx)
	if err != nil {
		return errors.WithMessage(err, "coordinator migrateShard")
	}

	clusterTopology := c.cluster.metaData.clusterTopology
	newShard := &clusterpb.Shard{
		Id:        newShardId,
		ShardRole: clusterpb.ShardRole_LEADER,
		Node:      targetNode.meta.Name,
	}
	clusterTopology.ShardView = append(clusterTopology.ShardView, newShard)

	// Update origin shard topologies
	shardTopologies, err := c.cluster.storage.ListShardTopologies(c.ctx, c.cluster.clusterID, []uint32{shard.Id})
	if err != nil {
		return errors.WithMessage(err, "coordinator migrateShard")
	}
	for i := 0; i < len(shardTopologies); i++ {
		if shardTopologies[i].ShardId == shard.Id {
			shardTopologies = append(shardTopologies, &clusterpb.ShardTopology{
				ShardId:  shard.Id,
				TableIds: originNodeTablesID,
				Version:  0,
			})
		}
	}

	// Create new shard topologies
	shardTopology := &clusterpb.ShardTopology{
		ShardId:  newShardId,
		TableIds: targetNodeTablesID,
		Version:  0,
	}

	if err := c.cluster.storage.PutShardTopology(c.ctx, c.cluster.clusterID, shardTopology.GetVersion(), shardTopology); err != nil {
		return errors.WithMessage(err, "coordinator migrateShard")
	}

	if err := c.cluster.storage.PutClusterTopology(c.ctx, c.cluster.clusterID, c.cluster.metaData.clusterTopology.Version, c.cluster.metaData.clusterTopology); err != nil {
		return errors.WithMessage(err, "coordinator migrateShard")
	}

	if err := c.cluster.Load(c.ctx); err != nil {
		return errors.WithMessage(err, "coordinator migrateShard")
	}

	// New shard created finish, migrate new shard to another node
	return c.migrateShard(newShard, targetNode)
}

func (c *coordinator) mergeShard(shards []*clusterpb.Shard, targetNode *Node) error {
	if len(shards) < 2 {
		return ErrMergeShard.WithCausef("The number of shards to be merged is at least 2")
	}

	// Check whether the shard is in the targetNode, migrate if shard is not in the targetNode
	shardIds := make([]uint32, 0)
	for i := 0; i < len(shards); i++ {
		if shards[i].Node != targetNode.meta.Name {
			shardIds = append(shardIds, shards[i].Id)
			if err := c.migrateShard(shards[i], targetNode); err != nil {
				return errors.WithMessage(err, "coordinator mergeShard")
			}
		}
	}

	// Now merge shards in the same node
	// Create a new shardTopology contains all tables in shards
	shardTopologies, err := c.cluster.storage.ListShardTopologies(c.ctx, c.cluster.clusterID, shardIds)
	if err != nil {
		return errors.Wrap(err, "coordinator migrateShard")
	}
	tableIds := make([]uint64, 0)
	for i := 0; i < len(shardTopologies); i++ {
		tableIds = append(tableIds, shardTopologies[i].TableIds...)
	}

	shardTopology := &clusterpb.ShardTopology{
		ShardId:  shards[0].Id,
		TableIds: tableIds,
		Version:  0,
	}

	if err := c.cluster.storage.PutShardTopology(c.ctx, c.cluster.clusterID, shardTopology.GetVersion(), shardTopology); err != nil {
		return errors.WithMessage(err, "coordinator migrateShard")
	}

	// Close merged shards
	if err := c.eventHandler.Dispatch(c.ctx, targetNode.meta.GetName(), &schedule.CloseEvent{ShardIDs: shardIds[1:]}); err != nil {
		return errors.WithMessage(err, "coordinator scatterShard")
	}

	// Update cluster topology shardView, delete closed shards
	closedShardMap := make(map[uint32]*clusterpb.Shard)
	for _, shard := range shards[1:] {
		closedShardMap[shard.Id] = shard
	}
	shardView := c.cluster.metaData.clusterTopology.ShardView
	for i := 0; i < len(shardView); i++ {
		if _, isPresent := closedShardMap[shardView[i].Id]; isPresent {
			shardView = append(shardView[:i], shardView[i+1:]...)
		}
	}

	if err := c.cluster.storage.PutClusterTopology(c.ctx, c.cluster.clusterID, c.cluster.metaData.clusterTopology.Version, c.cluster.metaData.clusterTopology); err != nil {
		return errors.WithMessage(err, "coordinator migrateShard")
	}

	return nil
}
