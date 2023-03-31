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
	PickNode(ctx context.Context, registerNodes []cluster.RegisteredNode) (cluster.RegisteredNode, error)
}

type RandomNodePicker struct{}

func NewRandomNodePicker() NodePicker {
	return &RandomNodePicker{}
}

func (p *RandomNodePicker) PickNode(_ context.Context, registerNodes []cluster.RegisteredNode) (cluster.RegisteredNode, error) {
	// Filter invalid nodes.
	numOnlineNodes := int64(0)
	for idx := range registerNodes {
		if registerNodes[idx].IsOnline() {
			numOnlineNodes++
		}
	}
	if numOnlineNodes < 0 {
		return cluster.RegisteredNode{}, ErrNodeNumberNotEnough
	}

	randSelectedIdx, err := rand.Int(rand.Reader, big.NewInt(int64(len(registerNodes))))
	if err != nil {
		return cluster.RegisteredNode{}, errors.WithMessage(err, "generate random node index")
	}
	selectedIdx := randSelectedIdx.Int64()
	for {
		if selectedIdx >= int64(len(registerNodes)) {
			// No valid node is found.
			return cluster.RegisteredNode{}, nil
		}

		if registerNodes[selectedIdx].IsOnline() {
			return registerNodes[selectedIdx], nil
		}
		selectedIdx++

		if selectedIdx >= int64(len(registerNodes)) {
			selectedIdx = 0
		}
	}
}
