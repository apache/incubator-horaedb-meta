// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import (
	"path"
	"strings"

	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/pingcap/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const delimiter = "/"

type etcdKVBase struct {
	client   *clientv3.Client
	rootPath string
}

// nolint:golint
// NewEtcdKVBase creates a new etcd kv.
func NewEtcdKVBase(client *clientv3.Client, rootPath string) *etcdKVBase {
	return &etcdKVBase{
		client:   client,
		rootPath: rootPath,
	}
}

func (kv *etcdKVBase) Load(key string) (string, error) {
	key = path.Join(kv.rootPath, key)

	resp, err := etcdutil.EtcdKVGet(kv.client, key)
	if err != nil {
		return "", err
	}
	if n := len(resp.Kvs); n == 0 {
		return "", nil
	} else if n > 1 {
		return "", etcdutil.ErrEtcdKVGetResponse.WithCausef("%v", resp.Kvs)
	}
	return string(resp.Kvs[0].Value), nil
}

func (kv *etcdKVBase) LoadRange(key, endKey string, limit int) ([]string, []string, error) {
	key = strings.Join([]string{kv.rootPath, key}, delimiter)
	endKey = strings.Join([]string{kv.rootPath, endKey}, delimiter)

	withRange := clientv3.WithRange(endKey)
	withLimit := clientv3.WithLimit(int64(limit))
	resp, err := etcdutil.EtcdKVGet(kv.client, key, withRange, withLimit)
	if err != nil {
		return nil, nil, err
	}
	keys := make([]string, 0, len(resp.Kvs))
	values := make([]string, 0, len(resp.Kvs))
	for _, item := range resp.Kvs {
		keys = append(keys, strings.TrimPrefix(strings.TrimPrefix(string(item.Key), kv.rootPath), delimiter))
		values = append(values, string(item.Value))
	}
	return keys, values, nil
}

func (kv *etcdKVBase) Save(key, value string) error {
	key = path.Join(kv.rootPath, key)
	txn := etcdutil.NewSlowLogTxn(kv.client)
	resp, err := txn.Then(clientv3.OpPut(key, value)).Commit()
	if err != nil {
		e := etcdutil.ErrEtcdKVPut.WithCause(err)
		log.Error("save to etcd meet error", zap.String("key", key), zap.String("value", value), zap.Error(e))
		return e
	}
	if !resp.Succeeded {
		return etcdutil.ErrEtcdTxnConflict
	}
	return nil
}

func (kv *etcdKVBase) Remove(key string) error {
	key = path.Join(kv.rootPath, key)

	txn := etcdutil.NewSlowLogTxn(kv.client)
	resp, err := txn.Then(clientv3.OpDelete(key)).Commit()
	if err != nil {
		err = etcdutil.ErrEtcdKVDelete.WithCause(err)
		log.Error("remove from etcd meet error", zap.String("key", key), zap.Error(err))
		return err
	}
	if !resp.Succeeded {
		return etcdutil.ErrEtcdTxnConflict
	}
	return nil
}
