// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrParseListSchemas  = coderr.NewCodeError(coderr.Internal, "meta storage list schemas")
	ErrParseCreateSchema = coderr.NewCodeError(coderr.Internal, "meta storage create schemas")
	ErrParsePutSchemas   = coderr.NewCodeError(coderr.Internal, "meta storage put schemas")

	ErrParseListClusters  = coderr.NewCodeError(coderr.Internal, "meta storage list cluster")
	ErrParseCreateCluster = coderr.NewCodeError(coderr.Internal, "meta storage create cluster")
	ErrParseGetCluster    = coderr.NewCodeError(coderr.Internal, "meta storage get cluster")
	ErrParsePutCluster    = coderr.NewCodeError(coderr.Internal, "meta storage put cluster")

	ErrParseCreateClusterTopology = coderr.NewCodeError(coderr.Internal, "meta storage create cluster topology")
	ErrParseGetClusterTopology    = coderr.NewCodeError(coderr.Internal, "meta storage get cluster topology")
	ErrParsePutClusterTopology    = coderr.NewCodeError(coderr.Internal, "meta storage put cluster topology")

	ErrParseCreateTable  = coderr.NewCodeError(coderr.Internal, "meta storage create tables")
	ErrParseGetTable     = coderr.NewCodeError(coderr.Internal, "meta storage get tables")
	ErrParseListTables   = coderr.NewCodeError(coderr.Internal, "meta storage list tables")
	ErrParsePutTables    = coderr.NewCodeError(coderr.Internal, "meta storage put tables")
	ErrParseDeleteTables = coderr.NewCodeError(coderr.Internal, "meta storage delete tables")

	ErrParseListShardTopology = coderr.NewCodeError(coderr.Internal, "meta storage list shard topology")
	ErrParsePutShardTopology  = coderr.NewCodeError(coderr.Internal, "meta storage put shard topology")

	ErrParseCreateOrUpdateNode = coderr.NewCodeError(coderr.Internal, "meta storage create or update nodes")
	ErrParseListNodes          = coderr.NewCodeError(coderr.Internal, "meta storage list nodes")
)
