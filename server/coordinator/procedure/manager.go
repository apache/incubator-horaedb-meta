// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
)

type Manager interface {
	SubmitScatterTask(ctx context.Context, request *ScatterRequest) error
	SubmitTransferLeaderTask(ctx context.Context, request *TransferLeaderRequest) error
	SubmitMigrateTask(ctx context.Context, request *MigrateRequest) error
	SubmitSplitTask(ctx context.Context, request *SplitRequest) error
	SubmitMergeTask(ctx context.Context, request *MergeRequest) error

	Cancel(ctx context.Context, procedureID uint64) error
	ListRunningProcedure(ctx context.Context) ([]*RunningProcedureInfo, error)
}

type ScatterRequest struct{}

// nolint
type TransferLeaderRequest struct {
	oldLeader *clusterpb.Shard
	newLeader *clusterpb.Shard
}

// nolint
type MigrateRequest struct {
	targetShard *clusterpb.Shard
	targetNode  *clusterpb.Node
}

// nolint
type SplitRequest struct {
	targetShard *clusterpb.Shard
}

// nolint
type MergeRequest struct {
	targetShards []*clusterpb.Shard
}
