// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/id"
	"github.com/pkg/errors"
)

type Factory struct {
	procedureIDAllocator id.Allocator
	dispatch             eventdispatch.Dispatch
	cluster              *cluster.Cluster
}

type ScatterRequest struct {
	ShardIDs []uint32
}

// nolint
type TransferLeaderRequest struct {
	OldLeader *clusterpb.Shard
	NewLeader *clusterpb.Shard
}

// nolint
type MigrateRequest struct {
	TargetShard *clusterpb.Shard
	TargetNode  *clusterpb.Node
}

// nolint
type SplitRequest struct {
	TargetShard *clusterpb.Shard
}

// nolint
type MergeRequest struct {
	TargetShards []*clusterpb.Shard
}

// nolint
func newFactory(allocator id.Allocator, dispatch eventdispatch.Dispatch, cluster *cluster.Cluster) *Factory {
	return &Factory{
		procedureIDAllocator: allocator,
		dispatch:             dispatch,
		cluster:              cluster,
	}
}

func (f *Factory) CreateScatterProcedure(ctx context.Context, request *ScatterRequest) (Procedure, error) {
	procedureID, err := f.allocProcedureID(ctx)
	if err != nil {
		return nil, errors.WithMessage(err, "alloc procedure id failed")
	}
	procedure := NewScatterProcedure(f.dispatch, f.cluster, procedureID, request.ShardIDs)
	return procedure, nil
}

func (f *Factory) CreateTransferLeaderProcedure(_ context.Context, _ *TransferLeaderRequest) {
}

func (f *Factory) CreateMigrateProcedure(_ context.Context, _ *MigrateRequest) {
}

func (f *Factory) CreateSplitProcedure(_ context.Context, _ *SplitRequest) {
}

func (f *Factory) CreateMergeProcedure(_ context.Context, _ *MergeRequest) {
}

func (f *Factory) allocProcedureID(ctx context.Context) (uint64, error) {
	return f.procedureIDAllocator.Alloc(ctx)
}
