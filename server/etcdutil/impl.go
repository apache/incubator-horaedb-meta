// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package etcdutil

import (
	"context"

	"go.uber.org/zap"

	"github.com/CeresDB/ceresmeta/pkg/log"
	clientv3 "go.etcd.io/etcd/client/v3"

	"go.etcd.io/etcd/server/v3/etcdserver"
)

type LeaderGetterWrapper struct {
	Server *etcdserver.EtcdServer
}

func (w *LeaderGetterWrapper) EtcdLeaderID() uint64 {
	return w.Server.Lead()
}

func Get(ctx context.Context, client *clientv3.Client, key string) (string, error) {
	resp, err := client.Get(ctx, key)
	if err != nil {
		return "", ErrEtcdKVGet.WithCause(err)
	}
	if n := len(resp.Kvs); n == 0 {
		return "", ErrEtcdKVGetNotFound
	} else if n > 1 {
		return "", ErrEtcdKVGetResponse.WithCausef("%v", resp.Kvs)
	}

	return string(resp.Kvs[0].Value), nil
}

func Scan(ctx context.Context, client *clientv3.Client, startKey, endKey string, limit int) ([]string, error) {
	withRange := clientv3.WithRange(endKey)
	withLimit := clientv3.WithLimit(int64(limit))
	values := make([]string, 0)

	for {
		resp, err := client.Get(ctx, startKey, withRange, withLimit)
		if err != nil {
			return nil, ErrEtcdKVGet.WithCause(err)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if len(values) > 0 {
			values = values[:len(values)-1]
		}

		for _, item := range resp.Kvs {
			values = append(values, string(item.Value))

			if string(item.Key) == endKey {
				log.Warn("scan value has reached end key", zap.String("endKey", endKey))
				return values, nil
			}

			startKey = string(item.Key)
		}

		if len(resp.Kvs) < limit {
			return values, nil
		}
	}
}
