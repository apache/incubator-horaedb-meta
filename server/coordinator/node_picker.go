// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator

import (
	"context"
	"time"

	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/hash"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
	"github.com/spaolacci/murmur3"
	"go.uber.org/zap"
)

type NodePicker interface {
	PickNode(ctx context.Context, shardIDs []storage.ShardID, shardTotalNum uint32, registerNodes []metadata.RegisteredNode) ([]storage.Node, error)
}
type UniformityConsistentHashNodePicker struct {
	logger *zap.Logger
}

func NewUniformityConsistentHashNodePicker(logger *zap.Logger) NodePicker {
	return &UniformityConsistentHashNodePicker{logger: logger}
}

type nodeMember string

func (m nodeMember) String() string {
	return string(m)
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return murmur3.Sum64(data)
}

func (p *UniformityConsistentHashNodePicker) PickNode(_ context.Context, shardID []storage.ShardID, shardTotalNum uint32, registerNodes []metadata.RegisteredNode) ([]metadata.RegisteredNode, error) {
	now := time.Now()

	aliveNodes := make([]metadata.RegisteredNode, 0, len(registerNodes))
	for _, registerNode := range registerNodes {
		if !registerNode.IsExpired(now) {
			aliveNodes = append(aliveNodes, registerNode)
		}
	}
	if len(aliveNodes) == 0 {
		return []metadata.RegisteredNode{}, errors.WithMessage(ErrPickNode, "no alive node in cluster")
	}

	mems := make([]hash.Member, 0, len(aliveNodes))
	for _, node := range aliveNodes {
		mems = append(mems, nodeMember(node.Node.Name))
	}

	consistentConfig := hash.Config{
		PartitionCount:    int(shardTotalNum),
		ReplicationFactor: int(shardTotalNum),
		Load:              1,
		Hasher:            hasher{},
	}
	c := hash.NewUniformityHash(mems, consistentConfig)
	for partID := 0; partID < consistentConfig.PartitionCount; partID++ {
		mem := c.GetPartitionOwner(partID)
		nodeName := mem.String()

		if storage.ShardID(partID) == shardID {
			p.logger.Debug("shard is founded in members", zap.Uint32("shardID", uint32(shardID)), zap.String("memberName", mem.String()))
			for _, node := range aliveNodes {
				if node.Node.Name == nodeName {
					return node, nil
				}
			}
		}
	}

	return []metadata.RegisteredNode{}, errors.WithMessagef(ErrPickNode, "no node is picked, shardID:%d, shardTotalNum:%d, aliveNodeNum:%d", shardID, shardTotalNum, len(aliveNodes))
}
