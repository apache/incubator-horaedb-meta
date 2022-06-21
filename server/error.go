// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package server

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrCreateEtcdClient = coderr.NewCodeErrorWrapper(coderr.Internal, "create etcd client")
	ErrStartEtcd        = coderr.NewCodeErrorWrapper(coderr.Internal, "start embed etcd")
)

var ErrStartEtcdTimeout = coderr.NewNormalizedCodeError(coderr.Internal, "start etcd server timeout")
