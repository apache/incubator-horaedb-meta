// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import (
	"context"
	"math"

	"github.com/CeresDB/ceresdbproto/pkg/metapb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type Options struct {
	// MaxScanLimit is the max limit of the number of keys in a scan.
	MaxScanLimit int
	// MinScanLimit is the min limit of the number of keys in a scan.
	MinScanLimit int
}

// MetaStorageImpl is the base underlying storage endpoint for all other upper
// specific storage backends. It should define some common storage interfaces and operations,
// which provIDes the default implementations for all kinds of storages.
type MetaStorageImpl struct {
	KV

	opts Options
}

// NewMetaStorageImpl creates a new base storage endpoint with the given KV and encryption key manager.
// It should be embedded insIDe a storage backend.
func NewMetaStorageImpl(
	kv KV,
	opts Options,
) *MetaStorageImpl {
	return &MetaStorageImpl{kv, opts}
}

// newEtcdBackend is used to create a new etcd backend.
func newEtcdStorage(client *clientv3.Client, rootPath string, opts Options) *MetaStorageImpl {
	return NewMetaStorageImpl(
		NewEtcdKV(client, rootPath), opts)
}

func (s *MetaStorageImpl) GetCluster(ctx context.Context, clusterID uint32) (*metapb.Cluster, error) {
	key := makeClusterKey(clusterID)
	value, err := s.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrapf(err, "get cluster err, key: %v", key)
	}
	meta := &metapb.Cluster{}
	if err = proto.Unmarshal([]byte(value), meta); err != nil {
		return nil, ErrMetaGetCluster.WithCausef("proto parse err: %v", err)
	}
	return meta, nil
}

func (s *MetaStorageImpl) PutCluster(ctx context.Context, clusterID uint32, meta *metapb.Cluster) error {
	value, err := proto.Marshal(meta)
	if err != nil {
		return ErrMetaPutCluster.WithCausef("proto parse err: %v", err)
	}
	key := makeClusterKey(clusterID)
	err = s.Put(ctx, key, string(value))
	if err != nil {
		return errors.Wrapf(err, "put cluster err, key: %v", key)
	}
	return nil
}

func (s *MetaStorageImpl) GetClusterTopology(ctx context.Context, clusterID uint32) (*metapb.ClusterTopology, error) {
	key := makeLatestVersion(clusterID)
	version, err := s.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrapf(err, "get cluster topology latest version err, key: %v", key)
	}
	key = makeClusterTopologyKey(clusterID, version)
	value, err := s.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrapf(err, "get cluster topology err, key: %v", key)
	}
	clusterMetaData := &metapb.ClusterTopology{}
	if err = proto.Unmarshal([]byte(value), clusterMetaData); err != nil {
		return nil, ErrMetaGetClusterTopology.WithCausef("proto parse err: %v", err)
	}
	return clusterMetaData, nil
}

func (s *MetaStorageImpl) PutClusterTopology(ctx context.Context, clusterID uint32, clusterMetaData *metapb.ClusterTopology) error {
	key := makeLatestVersion(clusterID)
	version, err := s.Get(ctx, key)
	if err != nil {
		return errors.Wrapf(err, "get cluster topology latest version err, key: %v", key)
	}
	value, err := proto.Marshal(clusterMetaData)
	if err != nil {
		return ErrMetaPutClusterTopology.WithCausef("proto parse err: %v", err)
	}
	key = makeClusterTopologyKey(clusterID, version)
	err = s.Put(ctx, key, string(value))
	if err != nil {
		return errors.Wrapf(err, "put cluster topology err, key: %v", key)
	}
	return nil
}

func (s *MetaStorageImpl) ListSchemas(ctx context.Context, clusterID uint32) ([]*metapb.Schema, error) {
	schemas := make([]*metapb.Schema, 0)
	nextID := uint32(0)
	endKey := makeSchemaKey(clusterID, math.MaxUint32)

	rangeLimit := s.opts.MaxScanLimit
	for {
		startKey := makeSchemaKey(clusterID, nextID)
		_, res, err := s.Scan(ctx, startKey, endKey, rangeLimit)
		if err != nil {
			if rangeLimit /= 2; rangeLimit >= s.opts.MinScanLimit {
				continue
			}
			return nil, errors.Wrapf(err, "get schemas err, start key: %v, end key: %v, range limit: %v", startKey, endKey, rangeLimit)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		for _, r := range res {
			schema := &metapb.Schema{}
			if err := proto.Unmarshal([]byte(r), schema); err != nil {
				return nil, ErrMetaGetSchemas.WithCausef("proto parse err: %v", err)
			}
			schemas = append(schemas, schema)
			if schema.GetId() == math.MaxUint32 {
				log.Warn("list schemas schema_id has reached max value", zap.Uint32("schema-id", schema.GetId()))
				return schemas, nil
			}
			nextID = schema.GetId() + 1
		}

		if len(res) < rangeLimit {
			return schemas, nil
		}
	}
}

func (s *MetaStorageImpl) PutSchemas(ctx context.Context, clusterID uint32, schemas []*metapb.Schema) error {
	for _, item := range schemas {
		key := makeSchemaKey(clusterID, item.Id)
		value, err := proto.Marshal(item)
		if err != nil {
			return ErrMetaPutSchemas.WithCausef("proto parse err: %v", err)
		}
		if err = s.Put(ctx, key, string(value)); err != nil {
			return errors.Wrapf(err, "put schemas err, key: %v", key)
		}
	}
	return nil
}

func (s *MetaStorageImpl) ListTables(ctx context.Context, clusterID uint32, schemaID uint32, tableID []uint64) ([]*metapb.Table, error) {
	tables := make([]*metapb.Table, 0)
	for _, item := range tableID {
		key := makeTableKey(clusterID, schemaID, item)
		value, err := s.Get(ctx, key)
		if err != nil {
			return nil, errors.Wrapf(err, "get tables err, key: %v", key)
		}
		tableData := &metapb.Table{}
		if err = proto.Unmarshal([]byte(value), tableData); err != nil {
			return nil, ErrMetaGetTables.WithCausef("proto parse err: %v", err)
		}
		tables = append(tables, tableData)
	}
	return tables, nil
}

func (s *MetaStorageImpl) PutTables(ctx context.Context, clusterID uint32, schemaID uint32, tables []*metapb.Table) error {
	for _, item := range tables {
		key := makeTableKey(clusterID, schemaID, item.Id)
		value, err := proto.Marshal(item)
		if err != nil {
			return ErrMetaPutTables.WithCausef("proto parse err: %v", err)
		}
		if err = s.Put(ctx, key, string(value)); err != nil {
			return errors.Wrapf(err, "put tables err, key: %v", key)
		}
	}
	return nil
}

func (s *MetaStorageImpl) DeleteTables(ctx context.Context, clusterID uint32, schemaID uint32, tableIDs []uint64) error {
	for _, item := range tableIDs {
		key := makeTableKey(clusterID, schemaID, item)
		if err := s.Delete(ctx, key); err != nil {
			return errors.Wrapf(err, "delete tables err, key: %v", key)
		}
	}
	return nil
}

func (s *MetaStorageImpl) ListShardTopologies(ctx context.Context, clusterID uint32, shardID []uint32) ([]*metapb.ShardTopology, error) {
	shardTableInfo := make([]*metapb.ShardTopology, 0)
	for _, item := range shardID {
		key := makeShardLatestVersion(clusterID, item)
		version, err := s.Get(ctx, key)
		if err != nil {
			return nil, errors.Wrapf(err, "get shard topology latest version err, key: %v", key)
		}
		key = makeShardKey(clusterID, item, version)
		value, err := s.Get(ctx, key)
		if err != nil {
			return nil, errors.Wrapf(err, "get shard topology err, key: %v", key)
		}
		shardTopology := &metapb.ShardTopology{}
		if err = proto.Unmarshal([]byte(value), shardTopology); err != nil {
			return nil, ErrMetaGetShardTopology.WithCausef("proto parse err: %v", err)
		}
		shardTableInfo = append(shardTableInfo, shardTopology)
	}
	return shardTableInfo, nil
}

func (s *MetaStorageImpl) PutShardTopologies(ctx context.Context, clusterID uint32, shardID []uint32, shardTableInfo []*metapb.ShardTopology) error {
	for index, item := range shardID {
		key := makeShardLatestVersion(clusterID, item)
		version, err := s.Get(ctx, key)
		if err != nil {
			return errors.Wrapf(err, "get shard topology err")
		}
		key = makeShardKey(clusterID, item, version)
		value, err := proto.Marshal(shardTableInfo[index])
		if err != nil {
			return ErrMetaPutShardTopology.WithCausef("proto parse err: %v", err)
		}
		if err = s.Put(ctx, key, string(value)); err != nil {
			return errors.Wrapf(err, "put shard topology err, key: %v", key)
		}
	}
	return nil
}

func (s *MetaStorageImpl) ListNodes(ctx context.Context, clusterID uint32) ([]*metapb.Node, error) {
	nodes := make([]*metapb.Node, 0)
	nextID := uint32(0)
	endKey := makeNodeKey(clusterID, math.MaxUint32)

	rangeLimit := s.opts.MaxScanLimit
	for {
		startKey := makeNodeKey(clusterID, nextID)
		_, res, err := s.Scan(ctx, startKey, endKey, rangeLimit)
		if err != nil {
			if rangeLimit /= 2; rangeLimit >= s.opts.MinScanLimit {
				continue
			}
			return nil, errors.Wrapf(err, "get nodes err, start key: %v, end key: %v, range limit: %v", startKey, endKey, rangeLimit)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		for _, r := range res {
			node := &metapb.Node{}
			if err := proto.Unmarshal([]byte(r), node); err != nil {
				return nil, ErrMetaGetNodes.WithCausef("proto parse err: %v", err)
			}
			nodes = append(nodes, node)
			if node.GetId() == math.MaxUint32 {
				log.Warn("list node node_id has reached max value", zap.Uint32("node-id", node.GetId()))
				return nodes, nil
			}
			nextID = node.GetId() + 1
		}

		if len(res) < rangeLimit {
			return nodes, nil
		}
	}
}

func (s *MetaStorageImpl) PutNodes(ctx context.Context, clusterID uint32, node []*metapb.Node) error {
	for _, item := range node {
		key := makeNodeKey(clusterID, item.Id)
		value, err := proto.Marshal(item)
		if err != nil {
			return ErrMetaPutNodes.WithCausef("proto parse err: %v", err)
		}
		if err = s.Put(ctx, key, string(value)); err != nil {
			return errors.Wrapf(err, "put nodes err, key: %v", key)
		}
	}
	return nil
}
