// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrMetaGetSchemas = coderr.NewCodeError(coderr.Internal, "meta storage get schemas")
	ErrMetaPutSchemas = coderr.NewCodeError(coderr.Internal, "meta storage put schemas")

	ErrMetaGetCluster = coderr.NewCodeError(coderr.Internal, "meta storage get cluster")
	ErrMetaPutCluster = coderr.NewCodeError(coderr.Internal, "meta storage put cluster")

	ErrMetaGetClusterTopology = coderr.NewCodeError(coderr.Internal, "meta storage get cluster topology")
	ErrMetaPutClusterTopology = coderr.NewCodeError(coderr.Internal, "meta storage put cluster topology")

	ErrMetaGetTables = coderr.NewCodeError(coderr.Internal, "meta storage get tables")
	ErrMetaPutTables = coderr.NewCodeError(coderr.Internal, "meta storage put tables")

	ErrMetaGetShardTopology = coderr.NewCodeError(coderr.Internal, "meta storage get shard topology")
	ErrMetaPutShardTopology = coderr.NewCodeError(coderr.Internal, "meta storage put shard topology")

	ErrMetaGetNodes = coderr.NewCodeError(coderr.Internal, "meta storage get nodes")
	ErrMetaPutNodes = coderr.NewCodeError(coderr.Internal, "meta storage put nodes")
)
