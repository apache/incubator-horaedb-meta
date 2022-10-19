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
	IDAllocator id.Allocator
	dispatch    eventdispatch.Dispatch
	cluster     *cluster.Cluster
}

type ScatterRequest struct {
	ShardIDs []uint32
}

// nolint
type TransferLeaderRequest struct {
	OldLeader *clusterpb.Shard
	NewLeader *clusterpb.Shard
}

type CreateTableRequest struct {
	SchemaName string
	NodeName   string
	CreateSQL  string
}

// nolint
func NewFactory(allocator id.Allocator, dispatch eventdispatch.Dispatch, cluster *cluster.Cluster) *Factory {
	return &Factory{
		IDAllocator: allocator,
		dispatch:    dispatch,
		cluster:     cluster,
	}
}

func (f *Factory) CreateScatterProcedure(cxt context.Context, request *ScatterRequest) (Procedure, error) {
	id, err := f.allocProcedureID(cxt)
	if err != nil {
		return nil, errors.WithMessage(err, "alloc procedure id")
	}
	procedure := NewScatterProcedure(f.dispatch, f.cluster, id, request.ShardIDs)
	return procedure, nil
}

func (f *Factory) CreateTransferLeaderProcedure(cxt context.Context, request *TransferLeaderRequest) (Procedure, error) {
	id, err := f.allocProcedureID(cxt)
	if err != nil {
		return nil, errors.WithMessage(err, "alloc procedure id")
	}
	procedure := NewTransferLeaderProcedure(f.dispatch, f.cluster, request.OldLeader, request.NewLeader, id)
	return procedure, nil
}

func (f *Factory) CreateCreateTableProcedure(cxt context.Context, request *CreateTableRequest) (Procedure, error) {
	id, err := f.allocProcedureID(cxt)
	if err != nil {
		return nil, errors.WithMessage(err, "alloc procedure id")
	}
	procedure := NewCreateTableProcedure(f.dispatch, f.cluster, id,
		&metaservicepb.CreateTableRequest{SchemaName: request.SchemaName, Name: request.NodeName, CreateSql: request.CreateSQL})
	return procedure, nil
}

func (f *Factory) allocProcedureID(ctx context.Context) (uint64, error) {
	return f.IDAllocator.Alloc(ctx)
}
