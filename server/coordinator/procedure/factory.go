// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
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

type CreateTableRequest struct {
	SchemaName string
	NodeName   string
	CreateSQL  string
}

// nolint
func newFactory(allocator id.Allocator, dispatch eventdispatch.Dispatch, cluster *cluster.Cluster) *Factory {
	return &Factory{
		procedureIDAllocator: allocator,
		dispatch:             dispatch,
		cluster:              cluster,
	}
}

func (f *Factory) CreateScatterProcedure(cxt context.Context, request *ScatterRequest) (Procedure, error) {
	procedureID, err := f.allocProcedureID(cxt)
	if err != nil {
		return nil, errors.WithMessage(err, "alloc procedure id failed")
	}
	procedure := NewScatterProcedure(f.dispatch, f.cluster, procedureID, request.ShardIDs)
	return procedure, nil
}

func (f *Factory) CreateTransferLeaderProcedure(_ context.Context, _ *TransferLeaderRequest) (Procedure, error) {
	return nil, nil
}

func (f *Factory) CreateMigrateProcedure(_ context.Context, _ *MigrateRequest) (Procedure, error) {
	return nil, nil
}

func (f *Factory) CreateSplitProcedure(_ context.Context, _ *SplitRequest) (Procedure, error) {
	return nil, nil
}

func (f *Factory) CreateMergeProcedure(_ context.Context, _ *MergeRequest) (Procedure, error) {
	return nil, nil
}

func (f *Factory) CreateCreateTableProcedure(cxt context.Context, request *CreateTableRequest) (Procedure, error) {
	procedureID, err := f.allocProcedureID(cxt)
	if err != nil {
		return nil, errors.WithMessage(err, "alloc procedure id failed")
	}
	procedure := NewCreateTableProcedure(f.dispatch, f.cluster, procedureID,
		&metaservicepb.CreateTableRequest{SchemaName: request.SchemaName, Name: request.NodeName, CreateSql: request.CreateSQL})
	return procedure, nil
}

func (f *Factory) allocProcedureID(ctx context.Context) (uint64, error) {
	return f.procedureIDAllocator.Alloc(ctx)
}
