// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrLoad            = coderr.NewCodeError(coderr.Internal, "cluster load")
	ErrClusterNotFound = coderr.NewCodeError(coderr.Internal, "cluster not found")
	ErrSchemaNotFound  = coderr.NewCodeError(coderr.Internal, "schema not found")
	ErrShardNotFound   = coderr.NewCodeError(coderr.Internal, "shard not found")
)
