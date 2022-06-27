// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package etcdutil

import clientv3 "go.etcd.io/etcd/client/v3"

type ClusterKV interface {
	clientv3.Cluster
	clientv3.KV
}

type EtcdLeaderGetter interface {
	GetLeader() uint64
}
