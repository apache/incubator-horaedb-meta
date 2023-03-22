// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"github.com/CeresDB/ceresmeta/server/storage"
)

type Manager interface {
	// Start must be called before manager is used.
	Start(ctx context.Context) error
	// Stop must be called before manager is dropped.
	Stop(ctx context.Context) error

	// Submit procedure to be executed asynchronously.
	// TODO: change result type, add channel to get whether the procedure executed successfully
	Submit(ctx context.Context, clusterID storage.ClusterID, procedure Procedure) error
	// ListRunningProcedure return immutable procedures info.
	ListRunningProcedure(ctx context.Context, clusterID storage.ClusterID) ([]*Info, error)
}
