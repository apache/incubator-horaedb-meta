// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator

import (
	"context"
	"strconv"
	"testing"

	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
)

const (
	NodeLength = 3
)

func TestRandomNodePicker(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()

	nodePicker := NewRandomNodePicker()

	var nodes []cluster.RegisteredNode
	_, err := nodePicker.PickNode(ctx, nodes)
	re.Error(err)

	for i := 0; i < NodeLength; i++ {
		nodes = append(nodes, cluster.RegisteredNode{
			Node:       storage.Node{Name: strconv.Itoa(i), State: storage.NodeStateOffline},
			ShardInfos: nil,
		})
	}
	_, err = nodePicker.PickNode(ctx, nodes)
	re.Error(err)

	for i := 0; i < NodeLength; i++ {
		nodes = append(nodes, cluster.RegisteredNode{
			Node:       storage.Node{Name: strconv.Itoa(i), State: storage.NodeStateOnline},
			ShardInfos: nil,
		})
	}
	_, err = nodePicker.PickNode(ctx, nodes)
	re.NoError(err)
}
