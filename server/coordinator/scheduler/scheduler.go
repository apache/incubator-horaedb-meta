// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package scheduler

import (
	"context"

	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/storage"
)

type Result struct {
	p procedure.Procedure
	// Scheduler will give the reason than why the procedure is generated.
	schedulerReason string
}

type Scheduler interface {
	// Schedule will generate procedure based on current cluster topology, which will be submitted to ProcedureManager,and whether it is actually executed depends on the current state of ProcedureManager.
	Schedule(ctx context.Context, clusterView storage.ClusterView, shardViews []storage.ShardView) (Result, error)
}
