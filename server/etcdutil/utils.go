// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package etcdutil

import (
	"context"
	"time"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	// DefaultRequestTimeout 10s is long enough for most of etcd clusters.
	DefaultRequestTimeout = 10 * time.Second

	// DefaultSlowRequestTime 1s for the threshold for normal request, for those
	// longer then 1s, they are considered as slow requests.
	DefaultSlowRequestTime = 1 * time.Second
)

// EtcdKVGet returns the etcd GetResponse by given key or key prefix
func EtcdKVGet(c *clientv3.Client, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	txn := NewSlowLogTxn(c)
	resp, err := txn.Then(clientv3.OpGet(key, opts...)).Commit()

	if err != nil {
		e := ErrEtcdKVGet.WithCause(err)
		log.Error("load from etcd meet error", zap.String("key", key))
		return nil, e
	}
	if !resp.Succeeded {
		return nil, ErrEtcdTxnConflict
	}
	return (*clientv3.GetResponse)(resp.Responses[0].GetResponseRange()), nil
}

// SlowLogTxn wraps etcd transaction and log slow one.
type SlowLogTxn struct {
	clientv3.Txn
	cancel context.CancelFunc
}

// NewSlowLogTxn create a SlowLogTxn.
func NewSlowLogTxn(client *clientv3.Client) clientv3.Txn {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	return &SlowLogTxn{
		Txn:    client.Txn(ctx),
		cancel: cancel,
	}
}

// If takes a list of comparison. If all comparisons passed in succeed,
// the operations passed into Then() will be executed. Or the operations
// passed into Else() will be executed.
func (t *SlowLogTxn) If(cs ...clientv3.Cmp) clientv3.Txn {
	t.Txn = t.Txn.If(cs...)
	return t
}

// Then takes a list of operations. The Ops list will be executed, if the
// comparisons passed in If() succeed.
func (t *SlowLogTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	t.Txn = t.Txn.Then(ops...)
	return t
}

// Commit implements Txn Commit interface.
func (t *SlowLogTxn) Commit() (*clientv3.TxnResponse, error) {
	start := time.Now()
	resp, err := t.Txn.Commit()
	t.cancel()

	cost := time.Since(start)
	if cost > DefaultSlowRequestTime {
		log.Warn("txn runs too slow",
			zap.Reflect("response", resp),
			zap.Duration("cost", cost),
			zap.Error(err))
	}

	return resp, errors.WithStack(err)
}
