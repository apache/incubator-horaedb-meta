package cluster

import "math/rand"

// clusterBalancer is used to collect cluster load, determine the node to select when migrating or splitting
type clusterBalancer struct {
	Cluster         Cluster
	balanceStrategy balanceStrategy
}

type balanceStrategy int32

const (
	StrategyRandom       = 0 // Select node from cluster random, only used to test
	StrategyShardBalance = 1
	StrategyTableBalance = 2
)

func (balancer *clusterBalancer) SelectNode() (*Node, error) {
	// default impl is select a random node
	switch balancer.balanceStrategy {
	case StrategyRandom:
		nodeCache := balancer.Cluster.nodesCache
		k := rand.Intn(len(nodeCache))
		i := 0
		for _, x := range nodeCache {
			if i == k {
				return x, nil
			}
			i++
		}
		break
	case StrategyShardBalance:
		break
	case StrategyTableBalance:
		break
	}

	return nil, nil
}
