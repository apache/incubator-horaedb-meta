// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator

import (
	"context"
	"strconv"
	"time"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/hash"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type NodePicker interface {
	PickNode(ctx context.Context, shardID storage.ShardID, registerNodes []metadata.RegisteredNode) (metadata.RegisteredNode, error)
}

type ConsistentHashNodePicker struct {
	hashReplicas int
}

func NewConsistentHashNodePicker(hashReplicas int) NodePicker {
	return &ConsistentHashNodePicker{hashReplicas: hashReplicas}
}

func (p *ConsistentHashNodePicker) PickNode(_ context.Context, shardID storage.ShardID, registerNodes []metadata.RegisteredNode) (metadata.RegisteredNode, error) {
	now := time.Now()

	hashRing := hash.New(p.hashReplicas, nil)
	nodeMapping := make(map[string]metadata.RegisteredNode, len(registerNodes))
	for _, registerNode := range registerNodes {
		if !registerNode.IsExpired(now) {
			nodeMapping[registerNode.Node.Name] = registerNode
			hashRing.Add(registerNode.Node.Name)
		}
	}

	log.Debug("pick node result", zap.Int("nodeNumber", len(nodeMapping)))
	if len(nodeMapping) == 0 {
		return metadata.RegisteredNode{}, errors.WithMessage(ErrNodeNumberNotEnough, "at least one online nodes is required")
	}

	pickedNodeName := hashRing.Get(strconv.Itoa(int(shardID)))

	return nodeMapping[pickedNodeName], nil
}
