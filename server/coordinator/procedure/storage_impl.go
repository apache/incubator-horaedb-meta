// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path"

	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	Version              = "v1"
	PathProcedure        = "procedure"
	PathDeletedProcedure = "deletedProcedure"
)

type EtcdStorageImpl struct {
	client    *clientv3.Client
	clusterID uint32
	rootPath  string
}

func NewEtcdStorageImpl(client *clientv3.Client, clusterID uint32, rootPath string) *EtcdStorageImpl {
	return &EtcdStorageImpl{
		client:    client,
		clusterID: clusterID,
		rootPath:  rootPath,
	}
}

// CreateOrUpdate example:
// procedure : /{rootPath}/v1/procedure/{clusterID}/{procedureID} -> {procedureType} + {procedureState} + {data}
func (e EtcdStorageImpl) CreateOrUpdate(ctx context.Context, meta *Meta) error {
	state := meta.State
	if state == StateFinished || state == StateCancelled {
		if err := e.deleteAndRecord(ctx, meta); err != nil {
			return errors.WithMessage(err, "etcd delete data failed")
		}
		return nil
	}

	str, err := encode(meta)
	if err != nil {
		return errors.WithMessage(err, "encode meta failed")
	}
	keyPath := e.generateNormalKeyPath(meta.ID)
	opPut := clientv3.OpPut(keyPath, str)

	if _, err = e.client.Do(ctx, opPut); err != nil {
		return errors.WithMessage(err, "etcd put data failed")
	}
	return nil
}

// Delete example:
// deletedProcedure : /{rootPath}/v1/historyProcedure/{clusterID}/{procedureID}
func (e EtcdStorageImpl) deleteAndRecord(ctx context.Context, meta *Meta) error {
	str, err := encode(meta)
	if err != nil {
		return errors.WithMessage(err, "encode meta failed")
	}
	keyPath := e.generateNormalKeyPath(meta.ID)
	deletedKeyPath := e.generateDeletedKeyPath(meta.ID)
	opDelete := clientv3.OpDelete(keyPath)
	opPut := clientv3.OpPut(deletedKeyPath, str)

	_, err = e.client.Txn(ctx).Then(opDelete, opPut).Commit()

	return err
}

func (e EtcdStorageImpl) ReadAllNeedRetry(ctx context.Context, batchSize int, metas *[]*Meta) error {
	do := func(_ string, value []byte) error {
		meta := &Meta{}
		if err := decode(meta, string(value)); err != nil {
			return errors.WithMessage(err, "decode meta failed")
		}

		*metas = append(*metas, meta)
		return nil
	}

	startKey := e.generateNormalKeyPath(uint64(0))
	endKey := e.generateNormalKeyPath(math.MaxUint64)

	err := etcdutil.Scan(ctx, e.client, startKey, endKey, batchSize, do)
	if err != nil {
		return errors.WithMessage(err, "scan procedure failed")
	}
	return nil
}

func (e EtcdStorageImpl) generateNormalKeyPath(procedureID uint64) string {
	return path.Join(e.rootPath, Version, PathProcedure, fmtID(uint64(e.clusterID)), fmtID(procedureID))
}

func (e EtcdStorageImpl) generateDeletedKeyPath(procedureID uint64) string {
	return path.Join(e.rootPath, Version, PathDeletedProcedure, fmtID(uint64(e.clusterID)), fmtID(procedureID))
}

func fmtID(id uint64) string {
	return fmt.Sprintf("%020d", id)
}

func encode(meta *Meta) (string, error) {
	bytes, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func decode(m *Meta, meta string) error {
	err := json.Unmarshal([]byte(meta), &m)
	return err
}
