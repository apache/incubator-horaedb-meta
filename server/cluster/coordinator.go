package cluster

import (
	"context"
	"sync"

	"github.com/CeresDB/ceresdbproto/pkg/metapb"
	"github.com/pkg/errors"
)

type coordinator struct {
	sync.Mutex
	cluster *Cluster
}

func (c *coordinator) Run(ctx context.Context) error {
	c.Lock()
	defer c.Unlock()
	if err := c.scatterShard(ctx); err != nil {
		return errors.Wrap(err, "coordinator Run")
	}
	return nil
}

// todo: consider ReplicationFactor
func (c *coordinator) scatterShard(ctx context.Context) error {
	if !(int(c.cluster.metaData.cluster.MinNodeCount) >= len(c.cluster.metaData.nodeMap) && c.cluster.metaData.clusterTopology.State == metapb.ClusterTopology_EMPTY) {
		return nil
	}

	c.cluster.Lock()
	defer c.cluster.Unlock()

	shardTotal := int(c.cluster.metaData.cluster.ShardTotal)
	minNodeCount := int(c.cluster.metaData.cluster.MinNodeCount)
	perNodeShardCount := shardTotal / minNodeCount
	shards := make([]*metapb.Shard, shardTotal)
	nodeIndex := 0
	var nodeList []*metapb.Node
	for _, v := range c.cluster.metaData.nodeMap {
		nodeList = append(nodeList, v)
	}

	for i := 0; i < minNodeCount; i++ {
		for j := 0; j < perNodeShardCount; j++ {
			if i*perNodeShardCount+j < shardTotal {
				for ; nodeIndex < len(nodeList); nodeIndex++ {
					// todo: consider nodes state
					shards = append(shards, &metapb.Shard{Id: uint32(i * j), ShardRole: metapb.ShardRole_LEADER, Node: nodeList[nodeIndex].Node})
				}
			}
		}
	}
	c.cluster.metaData.clusterTopology.ShardView = shards
	if err := c.cluster.storage.PutClusterTopology(ctx, c.cluster.clusterID, c.cluster.metaData.clusterTopology); err != nil {
		return errors.Wrap(err, "coordinator scatterShard")
	}
	if err := c.cluster.Load(ctx); err != nil {
		return errors.Wrap(err, "coordinator scatterShard")
	}
	return nil
}
