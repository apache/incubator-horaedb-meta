// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator

import (
	"context"
	"crypto/rand"
	"math/big"

	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/pkg/errors"
)

type NodePicker interface {
	PickNode(ctx context.Context, clusterName string) (cluster.RegisteredNode, error)
}

type RandomNodePicker struct {
	clusterManager cluster.Manager
}

func (p *RandomNodePicker) PickNode(ctx context.Context, clusterName string) (cluster.RegisteredNode, error) {
	nodes, err := p.clusterManager.ListRegisterNodes(ctx, clusterName)
	if err != nil {
		return cluster.RegisteredNode{}, err
	}

	var onlineNodes []cluster.RegisteredNode
	// Filter invalid nodes.
	for _, node := range nodes {
		if !node.IsOnline() {
			onlineNodes = append(onlineNodes, node)
		}
	}

	selectNodeIndex, err := rand.Int(rand.Reader, big.NewInt(int64(len(onlineNodes))))
	if err != nil {
		return cluster.RegisteredNode{}, errors.WithMessage(err, "generate random node index")
	}

	return onlineNodes[selectNodeIndex.Int64()], nil
}
