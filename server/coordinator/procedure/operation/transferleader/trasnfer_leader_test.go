// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package transferleader_test

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/operation/transferleader"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
	"github.com/stretchr/testify/require"
)

func TestTransferLeader(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	dispatch := test.MockDispatch{}
	c := test.InitEmptyCluster(ctx, t)
	s := test.NewTestStorage(t)

	snapshot := c.GetMetadata().GetClusterSnapshot()

	shardID := snapshot.Topology.ShardViews[0].ShardID
	newLeaderNodeName := snapshot.RegisteredNodes[0].Node.Name

	p, err := transferleader.NewProcedure(transferleader.ProcedureParams{
		ID:                0,
		Dispatch:          dispatch,
		Storage:           s,
		ClusterSnapShot:   snapshot,
		ShardID:           shardID,
		OldLeaderNodeName: "",
		NewLeaderNodeName: newLeaderNodeName,
	})
	re.NoError(err)

	err = p.Start(ctx)
	re.NoError(err)
}
