package scheduler

import (
	"context"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/storage"
)

type Scheduler interface {
	Schedule(ctx context.Context, clusterView storage.ClusterView, shardViews []storage.ShardView) (error, procedure.Procedure, string)
}
