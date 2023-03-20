package coordinator

import (
	"context"
	"crypto/rand"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/pkg/errors"
	"math/big"
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

	selectNodeIndex, err := rand.Int(rand.Reader, big.NewInt(int64(len(nodes))))
	if err != nil {
		return cluster.RegisteredNode{}, errors.WithMessage(err, "generate random node index")
	}

	return nodes[selectNodeIndex.Int64()], nil
}
