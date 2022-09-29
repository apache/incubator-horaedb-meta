// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package dispatch

import (
	"context"

	"github.com/CeresDB/ceresmeta/server/cluster"
)

type EventDispatch interface {
	OpenShard(context context.Context, address string, request *OpenShardRequest) error
	CloseShard(context context.Context, address string, request *CloseShardRequest) error
	CreateTableOnShard(context context.Context, address string, request *CreateTableOnShardRequest) error
	DropTableOnShard(context context.Context, address string, request *DropTableOnShardRequest) error
}

type OpenShardRequest struct {
	Shard *cluster.ShardInfo
}

type CloseShardRequest struct {
	ShardID uint32
}

type CreateTableOnShardRequest struct {
	TableInfo *cluster.TableInfo
	CreateSQL string
}

type DropTableOnShardRequest struct {
	ShardInfo   *cluster.ShardInfo
	PrevVersion uint64
	TableInfo   *cluster.TableInfo
}
