// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator_test

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
	"github.com/stretchr/testify/require"
)

func TestRandomShardPicker(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()

	c := test.InitStableCluster(ctx, t)
	snapshot := c.GetMetadata().GetClusterSnapshot()

	shardPicker := coordinator.NewRandomBalancedShardPicker()

	// ExpectShardNum is bigger than node number and enableDuplicateNode is true, it should return correct shards.
	shardNodes, err := shardPicker.PickShards(ctx, snapshot, 3)
	re.NoError(err)
	re.Equal(len(shardNodes), 3)
	shardNodes, err = shardPicker.PickShards(ctx, snapshot, 4)
	re.NoError(err)
	re.Equal(len(shardNodes), 4)
	// ExpectShardNum is bigger than shard number.
	_, err = shardPicker.PickShards(ctx, snapshot, 5)
	re.NoError(err)
}
