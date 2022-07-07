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
// which provIDes the default implementations for all kinds of storages.
type MetaStorageImpl struct {
	KV

	maxScanLimit int
	minScanLimit int
}

// NewMetaStorageImpl creates a new base storage endpoint with the given KV and encryption key manager.
// It should be embedded insIDe a storage backend.
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
func (s *MetaStorageImpl) GetCluster(clusterID uint32, meta *metapb.Cluster) (bool, error) {
	return false, nil
}
func (s *MetaStorageImpl) PutCluster(clusterID uint32, meta *metapb.Cluster) error {
	return nil
}

func (s *MetaStorageImpl) GetClusterTopology(clusterID uint32, clusterMetaData *metapb.ClusterTopology) (bool, error) {
	return false, nil
}
func (s *MetaStorageImpl) PutClusterTopology(clusterID uint32, clusterMetaData *metapb.ClusterTopology) error {
	return nil
}

func (s *MetaStorageImpl) GetSchemas(ctx context.Context, clusterID uint32, schemas []*metapb.Schema) error {
	nextID := uint32(0)
	endKey := schemaPath(clusterID, math.MaxUint32)

	rangeLimit := s.maxScanLimit
	for {
		startKey := schemaPath(clusterID, nextID)
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

			nextID = schema.GetID() + 1
		}

		if len(res) < rangeLimit {
			return nil
		}
	}
}

func (s *MetaStorageImpl) PutSchemas(ctx context.Context, clusterID uint32, schemas []*metapb.Schema) error {
	return nil
}

func (s *MetaStorageImpl) GetTables(clusterID uint32, schemaID uint32, tableID []uint64, table []*metapb.Table) (bool, error) {
	return false, nil
}

func (s *MetaStorageImpl) PutTables(clusterID uint32, schemaID uint32, tables []*metapb.Table) error {
	return nil
}

func (s *MetaStorageImpl) DeleteTables(clusterID uint32, schemaID uint32, tableIDs []uint64) (bool, error) {
	return false, nil
}

func (s *MetaStorageImpl) GetShardTopologies(clusterID uint32, shardID []uint32, shardTableInfo []*metapb.ShardTopology) (bool, error) {
	return false, nil
}

func (s *MetaStorageImpl) PutShardTopologies(clusterID uint32, shardID []uint32, shardTableInfo []*metapb.ShardTopology) error {
	return nil
}

func (s *MetaStorageImpl) GetNodes(clusterID uint32, node []*metapb.Node) (bool, error) {
	return false, nil
}

func (s *MetaStorageImpl) PutNodes(clusterID uint32, node []*metapb.Node) error {
	return nil
}
