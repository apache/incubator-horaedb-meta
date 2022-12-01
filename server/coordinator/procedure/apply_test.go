// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/stretchr/testify/require"
)

func TestApply(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	dispatch := MockDispatch{}
	_, c := prepare(t)
	s := NewTestStorage(t)

	getNodeShardsResult, err := c.GetNodeShards(ctx)
	re.NoError(err)

	// Apply procedure does not modify any data, but only uses the dispatch to synchronize the metadata, so it only ensures that no error is thrown.
	shardNeedToReopen := getNodeShardsResult.NodeShards[0]
	procedure := NewApplyProcedure(ApplyProcedureRequest{
		Dispatch:                   dispatch,
		ID:                         uint64(1),
		NodeName:                   shardNeedToReopen.ShardNode.NodeName,
		ShardsNeedToReopen:         []cluster.ShardInfo{shardNeedToReopen.ShardInfo},
		ShardsNeedToCloseAndReopen: []cluster.ShardInfo{},
		ShardNeedToClose:           []cluster.ShardInfo{},
		Storage:                    s,
	})
	err = procedure.Start(ctx)
	re.NoError(err)

	shardNeedToCloseAndReopen := getNodeShardsResult.NodeShards[0]
	procedure = NewApplyProcedure(ApplyProcedureRequest{
		Dispatch:                   dispatch,
		ID:                         uint64(1),
		NodeName:                   shardNeedToReopen.ShardNode.NodeName,
		ShardsNeedToReopen:         []cluster.ShardInfo{},
		ShardsNeedToCloseAndReopen: []cluster.ShardInfo{shardNeedToCloseAndReopen.ShardInfo},
		ShardNeedToClose:           []cluster.ShardInfo{},
		Storage:                    s,
	})
	err = procedure.Start(ctx)
	re.NoError(err)

	shardNeedToClose := getNodeShardsResult.NodeShards[0]
	procedure = NewApplyProcedure(ApplyProcedureRequest{
		Dispatch:                   dispatch,
		ID:                         uint64(1),
		NodeName:                   shardNeedToReopen.ShardNode.NodeName,
		ShardsNeedToReopen:         []cluster.ShardInfo{},
		ShardsNeedToCloseAndReopen: []cluster.ShardInfo{},
		ShardNeedToClose:           []cluster.ShardInfo{shardNeedToClose.ShardInfo},
		Storage:                    s,
	})
	err = procedure.Start(ctx)
	re.NoError(err)
}
