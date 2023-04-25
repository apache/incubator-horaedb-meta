// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator

import (
	"context"
	"crypto/rand"
	"math/big"
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

type RandomNodePicker struct{}

func NewRandomNodePicker() NodePicker {
	return &RandomNodePicker{}
}

func (p *RandomNodePicker) PickNode(_ context.Context, _ storage.ShardID, registeredNodes []metadata.RegisteredNode) (metadata.RegisteredNode, error) {
	now := time.Now().UnixMilli()

	onlineNodeLength := 0
	for _, registeredNode := range registeredNodes {
		if !registeredNode.IsExpired(now) {
			onlineNodeLength++
		}
	}

	if onlineNodeLength == 0 {
		return metadata.RegisteredNode{}, errors.WithMessage(ErrNodeNumberNotEnough, "online node length must bigger than 0")
	}

	randSelectedIdx, err := rand.Int(rand.Reader, big.NewInt(int64(onlineNodeLength)))
	if err != nil {
		return metadata.RegisteredNode{}, errors.WithMessage(err, "generate random node index")
	}
	selectIdx := int(randSelectedIdx.Int64())
	curOnlineIdx := -1
	for idx := 0; idx < len(registeredNodes); idx++ {
		if !registeredNodes[idx].IsExpired(now) {
			curOnlineIdx++
		}
		if curOnlineIdx == selectIdx {
			return registeredNodes[idx], nil
		}
	}

	return metadata.RegisteredNode{}, errors.WithMessage(ErrPickNode, "pick node failed")
}

type ConsistentHashNodePicker struct {
	hashReplicas int
}

func NewConsistentHashNodePicker(hashReplicas int) NodePicker {
	return &ConsistentHashNodePicker{hashReplicas: hashReplicas}
}

func (p *ConsistentHashNodePicker) PickNode(_ context.Context, shardID storage.ShardID, registerNodes []metadata.RegisteredNode) (metadata.RegisteredNode, error) {
	now := time.Now().UnixMilli()

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
		return metadata.RegisteredNode{}, errors.WithMessage(ErrNodeNumberNotEnough, "online node length must bigger than 0")
	}

	pickNodeName := hashRing.Get(strconv.Itoa(int(shardID)))

	return nodeMapping[pickNodeName], nil
}
