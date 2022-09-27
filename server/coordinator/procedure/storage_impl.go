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
	Version       = "v1"
	PathProcedure = "procedure"
)

type EtcdStorageImpl struct {
	client *clientv3.Client

	// opts Options
	clusterId uint32

	rootPath string
}

// example:
// procedure 1: v1/procedure/{ClusterID}/{procedureID} -> {procedureType} + {procedureState} + {data}

func (e EtcdStorageImpl) CreateOrUpdate(ctx context.Context, meta *Meta) error {
	keyPath := e.generateKeyPath(meta.ID)

	str, err := encode(meta)
	if err != nil {
		return errors.WithMessage(err, "encode meta failed")
	}
	opPutProcedure := clientv3.OpPut(keyPath, str)

	_, err = e.client.Txn(ctx).
		Then(opPutProcedure).
		Commit()

	return err
}

func (e EtcdStorageImpl) Scan(ctx context.Context, batchSize uint, state State, typ Typ) ([]*Meta, error) {

	metas := make([]*Meta, 0)
	do := func(_ string, value []byte) error {
		meta := &Meta{}
		if err := decode(meta, string(value)); err != nil {
			return errors.WithMessage(err, "decode meta failed")
		}

		metas = append(metas, meta)
		return nil
	}

	startKey := e.generateKeyPath(0)
	endKey := e.generateKeyPath(math.MaxUint64)

	err := etcdutil.Scan(ctx, e.client, startKey, endKey, math.MaxInt, do)
	if err != nil {
		return nil, errors.WithMessage(err, "scan procedure failed")
	}
	return metas, nil
}

/*
*
 */
func (e EtcdStorageImpl) generateKeyPath(procedureId uint64) string {
	return path.Join(e.rootPath, Version, PathProcedure, fmtID(uint64(e.clusterId)), fmtID(procedureId))
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
