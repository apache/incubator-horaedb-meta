// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
)

type Manager interface {
	// Start must be called before manager is used.
	Start(ctx context.Context) error
	// Stop must be called before manager is dropped.
	Stop(ctx context.Context) error

	Submit(ctx context.Context, procedure Procedure) (<-chan error, error)
	Cancel(ctx context.Context, procedureID uint64) error
	ListRunningProcedure(ctx context.Context) ([]*RunningProcedureInfo, error)
}
