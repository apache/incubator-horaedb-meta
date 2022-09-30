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
	PathHistoryProcedure = "historyProcedure"
)

type EtcdStorageImpl struct {
	client    *clientv3.Client
	clusterId uint32
	rootPath  string
}

func NewEtcdStorageImpl(client *clientv3.Client, clusterId uint32, rootPath string) *EtcdStorageImpl {
	return &EtcdStorageImpl{
		client:    client,
		clusterId: clusterId,
		rootPath:  rootPath,
	}
}

// CreateOrUpdate example:
// procedure : /{rootPath}/v1/procedure/{clusterID}/{procedureID} -> {procedureType} + {procedureState} + {data}
func (e EtcdStorageImpl) CreateOrUpdate(ctx context.Context, meta *Meta) error {
	str, err := encode(meta)
	if err != nil {
		return errors.WithMessage(err, "encode meta failed")
	}
	keyPath := e.generateKeyPath(meta.ID, false)
	opPut := clientv3.OpPut(keyPath, str)

	_, err = e.client.Txn(ctx).
		Then(opPut).
		Commit()

	return err
}

// Delete example:
// historyProcedure : /{rootPath}/v1/historyProcedure/{clusterID}/{procedureID}
func (e EtcdStorageImpl) Delete(ctx context.Context, meta *Meta) error {
	str, err := encode(meta)
	if err != nil {
		return errors.WithMessage(err, "encode meta failed")
	}
	keyPath := e.generateKeyPath(meta.ID, false)
	historyKeyPath := e.generateKeyPath(meta.ID, true)
	opDelete := clientv3.OpDelete(keyPath)
	opPut := clientv3.OpPut(historyKeyPath, str)

	_, err = e.client.Txn(ctx).Then(opDelete, opPut).Commit()

	return err
}

func (e EtcdStorageImpl) Scan(ctx context.Context, batchSize int) ([]*Meta, error) {
	metas := make([]*Meta, 0)
	do := func(_ string, value []byte) error {
		meta := &Meta{}
		if err := decode(meta, string(value)); err != nil {
			return errors.WithMessage(err, "decode meta failed")
		}

		metas = append(metas, meta)
		return nil
	}

	startKey := e.generateKeyPath(uint64(0), false)
	endKey := e.generateKeyPath(math.MaxUint64, false)

	err := etcdutil.Scan(ctx, e.client, startKey, endKey, batchSize, do)
	if err != nil {
		return nil, errors.WithMessage(err, "scan procedure failed")
	}
	return metas, nil
}

func (e EtcdStorageImpl) generateKeyPath(procedureId uint64, isHistory bool) string {
	var pathPrefix string
	if isHistory {
		pathPrefix = PathHistoryProcedure
	} else {
		pathPrefix = PathProcedure
	}
	return path.Join(e.rootPath, Version, pathPrefix, fmtID(uint64(e.clusterId)), fmtID(procedureId))
}

func fmtID(id uint64) string {
	return fmt.Sprintf("%020d", id)
}

func encode(meta *Meta) (string, error) {
	if bytes, err := json.Marshal(meta); err != nil {
		return "", err
	} else {
		return string(bytes), nil
	}
}

func decode(m *Meta, meta string) error {
	err := json.Unmarshal([]byte(meta), &m)
	return err
}
