// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import (
	"context"
	"math"
	"time"

	"github.com/CeresDB/ceresdbproto/pkg/metapb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
)

type Options struct {
	// maxScanLimit is the max limit of the number of keys in a scan.
	maxScanLimit int
	// minScanLimit is the min limit of the number of keys in a scan.
	minScanLimit int

	requestTimeout time.Duration
}

// MetaStorageImpl is the base underlying storage endpoint for all other upper
// specific storage backends. It should define some common storage interfaces and operations,
// which provides the default implementations for all kinds of storages.
type MetaStorageImpl struct {
	KV

	maxScanLimit int
	minScanLimit int
}

// NewMetaStorageImpl creates a new base storage endpoint with the given KV and encryption key manager.
// It should be embedded inside a storage backend.
func NewMetaStorageImpl(
	kv KV,
	opts Options,
) *MetaStorageImpl {
	return &MetaStorageImpl{
		kv,
		opts.maxScanLimit,
		opts.minScanLimit,
	}
}

// newEtcdBackend is used to create a new etcd backend.
func newEtcdStorage(client *clientv3.Client, rootPath string, opts Options) *MetaStorageImpl {
	return NewMetaStorageImpl(
		NewEtcdKV(client, rootPath, opts.requestTimeout), opts)
}
func (s *MetaStorageImpl) GetCluster(clusterId uint32, meta *metapb.Cluster) (bool, error) {
	return false, nil
}
func (s *MetaStorageImpl) PutCluster(clusterId uint32, meta *metapb.Cluster) error {
	return nil
}

func (s *MetaStorageImpl) GetClusterTopology(clusterId uint32, clusterMetaData *metapb.ClusterTopology) (bool, error) {
	return false, nil
}
func (s *MetaStorageImpl) PutClusterTopology(clusterId uint32, clusterMetaData *metapb.ClusterTopology) error {
	return nil
}

func (s *MetaStorageImpl) GetSchemas(ctx context.Context, clusterId uint32, schemas []*metapb.Schema) error {
	nextID := uint32(0)
	endKey := schemaPath(clusterId, math.MaxUint32)

	rangeLimit := s.maxScanLimit
	for {
		startKey := schemaPath(clusterId, nextID)
		_, res, err := s.Scan(startKey, endKey, rangeLimit)
		if err != nil {
			if rangeLimit /= 2; rangeLimit >= s.minScanLimit {
				continue
			}
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		for _, r := range res {
			schema := &metapb.Schema{}
			if err := proto.Unmarshal([]byte(r), schema); err != nil {
				return ErrMetaGetSchemas.WithCausef("proto parse err:%v", err)
			}

			nextID = schema.GetId() + 1
		}

		if len(res) < rangeLimit {
			return nil
		}
	}
}

func (s *MetaStorageImpl) PutSchemas(ctx context.Context, clusterId uint32, schemas []*metapb.Schema) error {
	return nil
}

func (s *MetaStorageImpl) GetTables(clusterId uint32, schemaId uint32, tableId []uint64, table []*metapb.Table) (bool, error) {
	return false, nil
}

func (s *MetaStorageImpl) PutTables(clusterId uint32, schemaId uint32, tables []*metapb.Table) error {
	return nil
}

func (s *MetaStorageImpl) DeleteTables(clusterId uint32, schemaId uint32, tableIds []uint64) (bool, error) {
	return false, nil
}

func (s *MetaStorageImpl) GetShardTopologies(clusterId uint32, shardId []uint32, shardTableInfo []*metapb.ShardTopology) (bool, error) {
	return false, nil
}

func (s *MetaStorageImpl) PutShardTopologies(clusterId uint32, shardId []uint32, shardTableInfo []*metapb.ShardTopology) error {
	return nil
}

func (s *MetaStorageImpl) GetNodes(clusterId uint32, node []*metapb.Node) (bool, error) {
	return false, nil
}

func (s *MetaStorageImpl) PutNodes(clusterId uint32, node []*metapb.Node) error {
	return nil
}
