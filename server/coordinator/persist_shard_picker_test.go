package coordinator_test

import (
	"context"
	"github.com/apache/incubator-horaedb-meta/server/cluster/metadata"
	"github.com/apache/incubator-horaedb-meta/server/coordinator"
	"github.com/apache/incubator-horaedb-meta/server/coordinator/procedure/test"
	"github.com/apache/incubator-horaedb-meta/server/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPersistShardPicker(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()

	c := test.InitStableCluster(ctx, t)

	persistShardPicker := coordinator.NewPersistShardPicker(c.GetMetadata(), coordinator.NewLeastTableShardPicker())
	pickResult, err := persistShardPicker.PickShards(ctx, c.GetMetadata().GetClusterSnapshot(), test.TestSchemaName, []string{test.TestTableName0})
	re.NoError(err)
	re.Equal(len(pickResult), 1)

	createResult, err := c.GetMetadata().CreateTable(ctx, metadata.CreateTableRequest{
		ShardID:       pickResult[test.TestTableName0].ID,
		LatestVersion: 0,
		SchemaName:    test.TestSchemaName,
		TableName:     test.TestTableName0,
		PartitionInfo: storage.PartitionInfo{Info: nil},
	})
	re.NoError(err)
	re.Equal(test.TestTableName0, createResult.Table.Name)

	// Try to pick shard for same table after the table is created.
	newPickResult, err := persistShardPicker.PickShards(ctx, c.GetMetadata().GetClusterSnapshot(), test.TestSchemaName, []string{test.TestTableName0})
	re.NoError(err)
	re.Equal(len(newPickResult), 1)
	re.Equal(newPickResult[test.TestTableName0], pickResult[test.TestTableName0])

	// Try to pick shard for another table.
	pickResult, err = persistShardPicker.PickShards(ctx, c.GetMetadata().GetClusterSnapshot(), test.TestSchemaName, []string{test.TestTableName1})
	re.NoError(err)
	re.Equal(len(pickResult), 1)

	err = c.GetMetadata().DropTable(ctx, metadata.DropTableRequest{
		SchemaName:    test.TestSchemaName,
		TableName:     test.TestTableName0,
		ShardID:       pickResult[test.TestTableName0].ID,
		LatestVersion: 0,
	})
	re.NoError(err)

	// Try to pick shard for table1 after drop table0.
	newPickResult, err = persistShardPicker.PickShards(ctx, c.GetMetadata().GetClusterSnapshot(), test.TestSchemaName, []string{test.TestTableName1})
	re.NoError(err)
	re.Equal(len(pickResult), 1)
	re.Equal(newPickResult[test.TestTableName1], pickResult[test.TestTableName1])

	err = c.GetMetadata().DeleteAssignTable(ctx, test.TestSchemaName, test.TestTableName1)
	re.NoError(err)

	// Try to pick another for table1 after drop table1 assign result.
	newPickResult, err = persistShardPicker.PickShards(ctx, c.GetMetadata().GetClusterSnapshot(), test.TestSchemaName, []string{test.TestTableName1})
	re.NoError(err)
	re.Equal(len(pickResult), 1)
	re.NotEqual(newPickResult[test.TestTableName1], pickResult[test.TestTableName1])
}
