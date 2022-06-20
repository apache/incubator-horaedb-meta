package server

import "github.com/CeresDB/ceresmeta/coderr"

var (
	ErrCreateEtcdClient = coderr.NewCodeErrorWrapper(coderr.Internal, "fail to create etcd client")
	ErrStartEtcd        = coderr.NewCodeErrorWrapper(coderr.Internal, "fail to start embed etcd")
)
var ErrStartEtcdTimeout = coderr.NewNormalizedCodeError(coderr.Internal, "fail to start etcd server in time")
