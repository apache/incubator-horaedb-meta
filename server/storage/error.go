// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrEncodeCluster     = coderr.NewCodeError(coderr.Internal, "meta storage unmarshal cluster")
	ErrDecodeCluster     = coderr.NewCodeError(coderr.Internal, "meta storage marshal cluster")
	ErrEncodeClusterView = coderr.NewCodeError(coderr.Internal, "meta storage unmarshal cluster view")
	ErrDecodeClusterView = coderr.NewCodeError(coderr.Internal, "meta storage marshal cluster view")
	ErrEncodeShardView   = coderr.NewCodeError(coderr.Internal, "meta storage unmarshal shard view")
	ErrDecodeShardView   = coderr.NewCodeError(coderr.Internal, "meta storage marshal shard view")
	ErrEncodeSchema      = coderr.NewCodeError(coderr.Internal, "meta storage unmarshal schema")
	ErrDecodeSchema      = coderr.NewCodeError(coderr.Internal, "meta storage marshal schema")
	ErrEncodeTable       = coderr.NewCodeError(coderr.Internal, "meta storage unmarshal table")
	ErrDecodeTable       = coderr.NewCodeError(coderr.Internal, "meta storage marshal table")
	ErrEncodeNode        = coderr.NewCodeError(coderr.Internal, "meta storage unmarshal node")
	ErrDecodeNode        = coderr.NewCodeError(coderr.Internal, "meta storage marshal node")

	ErrCreateSchemaAgain      = coderr.NewCodeError(coderr.Internal, "meta storage create schemas")
	ErrCreateClusterAgain     = coderr.NewCodeError(coderr.Internal, "meta storage create cluster")
	ErrCreateClusterViewAgain = coderr.NewCodeError(coderr.Internal, "meta storage create cluster view")
	ErrPutClusterViewConflict = coderr.NewCodeError(coderr.Internal, "meta storage put cluster view")
	ErrCreateTableAgain       = coderr.NewCodeError(coderr.Internal, "meta storage create tables")
	ErrDeleteTableAgain       = coderr.NewCodeError(coderr.Internal, "meta storage delete table")
	ErrCreateShardViewAgain   = coderr.NewCodeError(coderr.Internal, "meta storage create shard view")
	ErrPutShardViewConflict   = coderr.NewCodeError(coderr.Internal, "meta storage put shard view")
)
