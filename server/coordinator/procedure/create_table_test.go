// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/stretchr/testify/require"
)

const testTableName = "testTable"
const testSchemaName = "testSchemaName"
const testNodeName = "testNode"
const testCluster = "testCluster"

func TestCreateTable(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	dispatch := MockDispatch{}
	cluster := newTestCluster(ctx, t)

	// Initialize shard topology
	shardVies := make([]*clusterpb.Shard, 0)
	shard0 := &clusterpb.Shard{
		ShardRole: clusterpb.ShardRole_LEADER,
		Node:      nodeName0,
		Id:        0,
	}
	shardVies = append(shardVies, shard0)
	shard1 := &clusterpb.Shard{
		ShardRole: clusterpb.ShardRole_LEADER,
		Node:      nodeName0,
		Id:        1,
	}
	shardVies = append(shardVies, shard1)
	err := cluster.UpdateClusterTopology(ctx, clusterpb.ClusterTopology_STABLE, shardVies)
	re.NoError(err)

	procedure := NewCreateTableProcedure(dispatch, cluster, uint64(1), &metaservicepb.CreateTableRequest{
		Header: &metaservicepb.RequestHeader{
			Node:        testNodeName,
			ClusterName: testCluster,
		},
		SchemaName: testSchemaName,
		Name:       testTableName,
		CreateSql:  "",
	})
	err = procedure.Start(ctx)
	re.NoError(err)
}
