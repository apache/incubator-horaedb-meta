package server

import "github.com/CeresDB/ceresmeta/codeerr"

var (
	ErrCreateEtcdClient = codeerr.NewCodeErrorWrapper(codeerr.Internal, "fail to create etcd client")
	ErrStartEtcd        = codeerr.NewCodeErrorWrapper(codeerr.Internal, "fail to start embed etcd")
)
var ErrStartEtcdTimeout = codeerr.NewNormalizedCodeError(codeerr.Internal, "fail to start etcd server in time")
