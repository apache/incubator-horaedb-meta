// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var ErrMetaGetSchemas = coderr.NewCodeError(coderr.Internal, "meta storage get schemas")
var ErrMetaPutSchemas = coderr.NewCodeError(coderr.Internal, "meta storage put schemas")

var ErrMetaGetCluster = coderr.NewCodeError(coderr.Internal, "meta storage get cluster")
var ErrMetaPutCluster = coderr.NewCodeError(coderr.Internal, "meta storage put cluster")

var ErrMetaGetClusterTopology = coderr.NewCodeError(coderr.Internal, "meta storage get cluster topology")
var ErrMetaPutClusterTopology = coderr.NewCodeError(coderr.Internal, "meta storage put cluster topology")

var ErrMetaGetTables = coderr.NewCodeError(coderr.Internal, "meta storage get tables")
var ErrMetaPutTables = coderr.NewCodeError(coderr.Internal, "meta storage put tables")

var ErrMetaGetShardTopology = coderr.NewCodeError(coderr.Internal, "meta storage get shard topology")
var ErrMetaPutShardTopology = coderr.NewCodeError(coderr.Internal, "meta storage put shard topology")

var ErrMetaGetNodes = coderr.NewCodeError(coderr.Internal, "meta storage get nodes")
var ErrMetaPutNodes = coderr.NewCodeError(coderr.Internal, "meta storage put nodes")
