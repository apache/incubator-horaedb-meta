// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrLoad            = coderr.NewCodeError(coderr.Internal, "cluster load")
	ErrClusterNotFound = coderr.NewCodeError(coderr.NotFound, "cluster not found")
	ErrSchemaNotFound  = coderr.NewCodeError(coderr.NotFound, "schema not found")
	ErrShardNotFound   = coderr.NewCodeError(coderr.NotFound, "shard not found")
)
