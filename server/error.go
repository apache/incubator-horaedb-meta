package server

import (
	coderr2 "github.com/CeresDB/ceresmeta/pkg/coderr"
)

var (
	ErrCreateEtcdClient = coderr2.NewCodeErrorWrapper(coderr2.Internal, "fail to create etcd client")
	ErrStartEtcd        = coderr2.NewCodeErrorWrapper(coderr2.Internal, "fail to start embed etcd")
)
var ErrStartEtcdTimeout = coderr2.NewNormalizedCodeError(coderr2.Internal, "fail to start etcd server in time")
