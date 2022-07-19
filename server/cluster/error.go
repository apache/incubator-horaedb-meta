// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrClusterAlreadyExists    = coderr.NewCodeError(coderr.Internal, "cluster already exists")
	ErrClusterNotFound         = coderr.NewCodeError(coderr.NotFound, "cluster not found")
	ErrClusterTopologyNotFound = coderr.NewCodeError(coderr.NotFound, "cluster not found")
	ErrSchemaNotFound          = coderr.NewCodeError(coderr.NotFound, "schemas not found")
	ErrTableNotFound           = coderr.NewCodeError(coderr.NotFound, "table not found")
	ErrShardNotFound           = coderr.NewCodeError(coderr.NotFound, "shard not found")
	ErrNodeNotFound            = coderr.NewCodeError(coderr.NotFound, "node not found")
)
