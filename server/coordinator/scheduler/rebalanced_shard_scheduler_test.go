package scheduler_test

import (
	"context"
	"github.com/CeresDB/ceresmeta/server/coordinator"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/test"
	"github.com/CeresDB/ceresmeta/server/coordinator/scheduler"
	"go.uber.org/zap"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRebalancedScheduler(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()

	procedureFactory := coordinator.NewFactory(test.MockIDAllocator{}, test.MockDispatch{}, test.NewTestStorage(t))

	s := scheduler.NewRebalancedShardScheduler(zap.NewNop(), procedureFactory, coordinator.NewConsistentHashNodePicker(zap.NewNop(), 50))

	// EmptyCluster would be scheduled an empty procedure.
	emptyCluster := test.InitEmptyCluster(ctx, t)
	result, err := s.Schedule(ctx, emptyCluster.GetMetadata().GetClusterSnapshot())
	re.NoError(err)
	re.Nil(result.Procedure)

	// PrepareCluster would be scheduled an empty procedure.
	prepareCluster := test.InitPrepareCluster(ctx, t)
	result, err = s.Schedule(ctx, prepareCluster.GetMetadata().GetClusterSnapshot())
	re.NoError(err)
	re.Nil(result.Procedure)

	// StableCluster with all shards assigned would be scheduled a load balance procedure.
	stableCluster := test.InitStableCluster(ctx, t)
	result, err = s.Schedule(ctx, stableCluster.GetMetadata().GetClusterSnapshot())
	re.NoError(err)
	re.NotNil(result.Procedure)
}
