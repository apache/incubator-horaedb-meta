// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import (
	"context"
	"math"
	"path"
	"strconv"
	"time"

	"go.etcd.io/etcd/client/v3/clientv3util"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
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

// metaStorageImpl is the base underlying storage endpoint for all other upper
// specific storage backends. It should define some common storage interfaces and operations,
// which provIDes the default implementations for all kinds of storages.
type metaStorageImpl struct {
	KV

	opts Options

	rootPath string
}

// NewMetaStorageImpl creates a new base storage endpoint with the given KV and encryption key manager.
// It should be embedded insIDe a storage backend.
func NewMetaStorageImpl(
	kv KV,
	opts Options,
	rootPath string,
) Storage {
	return &metaStorageImpl{kv, opts, rootPath}
}

// newEtcdBackend is used to create a new etcd backend.
func newEtcdStorage(client *clientv3.Client, rootPath string, opts Options) Storage {
	return NewMetaStorageImpl(
		NewEtcdKV(client, rootPath), opts, rootPath)
}

func (s *metaStorageImpl) ListClusters(ctx context.Context) ([]*clusterpb.Cluster, error) {
	clusters := make([]*clusterpb.Cluster, 0)
	nextID := uint32(0)
	endKey := makeClusterKey(math.MaxUint32)

	rangeLimit := s.opts.MaxScanLimit
	for {
		startKey := makeClusterKey(nextID)
		_, res, err := s.Scan(ctx, startKey, endKey, rangeLimit)
		if err != nil {
			if rangeLimit /= 2; rangeLimit >= s.opts.MinScanLimit {
				continue
			}
			return nil, errors.Wrapf(err, "fail to list clusters, start key:%s, end key:%s, range limit:%d", startKey, endKey, rangeLimit)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		for _, r := range res {
			cluster := &clusterpb.Cluster{}
			if err := proto.Unmarshal([]byte(r), cluster); err != nil {
				return nil, ErrParseListClusters.WithCausef("fail to parse proto, err:%v", err)
			}

			clusters = append(clusters, cluster)
			if cluster.GetId() == math.MaxUint32 {
				log.Warn("list clusters cluster_id has reached max value", zap.Uint32("cluster-id", cluster.GetId()))
				return clusters, nil
			}
			nextID = cluster.GetId() + 1
		}

		if len(res) < rangeLimit {
			return clusters, nil
		}
	}
}

func (s *metaStorageImpl) CreateCluster(ctx context.Context, cluster *clusterpb.Cluster) (*clusterpb.Cluster, error) {
	now := time.Now()
	cluster.CreatedAt = uint64(now.Unix())

	value, err := proto.Marshal(cluster)
	if err != nil {
		return nil, ErrParseCreateCluster.WithCausef("fail to parse proto，clusterID:%d, err:%v", cluster.Id, err)
	}

	key := path.Join(s.rootPath, makeClusterKey(cluster.Id))

	// Check if the key exists, if not，create cluster; Otherwise, the cluster already exists and return an error.
	cmp := clientv3util.KeyMissing(key)
	opCreateCluster := clientv3.OpPut(key, string(value))

	resp, err := s.Txn(ctx).
		If(cmp).
		Then(opCreateCluster).
		Commit()
	if err != nil {
		return nil, errors.Wrapf(err, "fail to create cluster, clusterID:%d, key:%s", cluster.Id, key)
	}
	if !resp.Succeeded {
		return nil, ErrParseCreateCluster.WithCausef("cluster may already exist, clusterID:%d, resp:%v", cluster.Id, resp)
	}
	return cluster, nil
}

func (s *metaStorageImpl) GetCluster(ctx context.Context, clusterID uint32) (*clusterpb.Cluster, error) {
	key := makeClusterKey(clusterID)
	value, err := s.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrapf(err, "fail to get cluster, clusterID:%d, key:%s", clusterID, key)
	}

	cluster := &clusterpb.Cluster{}
	if err = proto.Unmarshal([]byte(value), cluster); err != nil {
		return nil, ErrParseGetCluster.WithCausef("fail to parse proto, clusterID:%d, err:%v", clusterID, err)
	}
	return cluster, nil
}

func (s *metaStorageImpl) PutCluster(ctx context.Context, clusterID uint32, cluster *clusterpb.Cluster) error {
	value, err := proto.Marshal(cluster)
	if err != nil {
		return ErrParsePutCluster.WithCausef("fail to parse proto, clusterID:%d, err:%v", clusterID, err)
	}

	key := makeClusterKey(clusterID)
	err = s.Put(ctx, key, string(value))
	if err != nil {
		return errors.Wrapf(err, "fail to put cluster, clusterID:%d, key:%s", clusterID, key)
	}
	return nil
}

func (s *metaStorageImpl) CreateClusterTopology(ctx context.Context, clusterTopology *clusterpb.ClusterTopology) (*clusterpb.ClusterTopology, error) {
	now := time.Now()
	clusterTopology.CreatedAt = uint64(now.Unix())

	value, err := proto.Marshal(clusterTopology)
	if err != nil {
		return nil, ErrParseCreateClusterTopology.WithCausef("fail to parse proto, clusterID:%d, err:%v", clusterTopology.ClusterId, err)
	}

	key := path.Join(s.rootPath, makeClusterTopologyKey(clusterTopology.ClusterId, fmtID(clusterTopology.DataVersion)))
	latestVersionKey := path.Join(s.rootPath, makeClusterTopologyLatestVersionKey(clusterTopology.ClusterId))

	// Check if the key and latest version key exists, if not，create cluster topology and latest version; Otherwise, the cluster topology already exists and return an error.
	cmpLatestVersionKey := clientv3util.KeyMissing(latestVersionKey)
	cmpKey := clientv3util.KeyMissing(key)
	opCreateClusterTopology := clientv3.OpPut(key, string(value))
	opCreateClusterTopologyLatestVersion := clientv3.OpPut(latestVersionKey, fmtID(clusterTopology.DataVersion))

	resp, err := s.Txn(ctx).
		If(cmpLatestVersionKey, cmpKey).
		Then(opCreateClusterTopology, opCreateClusterTopologyLatestVersion).
		Commit()
	if err != nil {
		return nil, errors.Wrapf(err, "fail to create cluster topology, clusterID:%d, key:%s", clusterTopology.ClusterId, key)
	}
	if !resp.Succeeded {
		return nil, ErrParseCreateClusterTopology.WithCausef("cluster topology may already exist, clusterID:%d, resp:%v", clusterTopology.ClusterId, resp)
	}
	return clusterTopology, nil
}

func (s *metaStorageImpl) GetClusterTopology(ctx context.Context, clusterID uint32) (*clusterpb.ClusterTopology, error) {
	key := makeClusterTopologyLatestVersionKey(clusterID)
	version, err := s.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrapf(err, "fail to get cluster topology latest version, clusterID:%d, key:%s", clusterID, key)
	}

	key = makeClusterTopologyKey(clusterID, version)
	value, err := s.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrapf(err, "fail to get cluster topology, clusterID:%d, key:%s", clusterID, key)
	}

	clusterTopology := &clusterpb.ClusterTopology{}
	if err = proto.Unmarshal([]byte(value), clusterTopology); err != nil {
		return nil, ErrParseGetClusterTopology.WithCausef("fail to parse proto, clusterID:%d, err:%v", clusterID, err)
	}
	return clusterTopology, nil
}

func (s *metaStorageImpl) PutClusterTopology(ctx context.Context, clusterID uint32, latestVersion uint64, clusterTopology *clusterpb.ClusterTopology) error {
	value, err := proto.Marshal(clusterTopology)
	if err != nil {
		return ErrParsePutClusterTopology.WithCausef("fail to parse proto, clusterID:%d, err:%v", clusterID, err)
	}

	key := path.Join(s.rootPath, makeClusterTopologyKey(clusterID, fmtID(clusterTopology.DataVersion)))
	latestVersionKey := path.Join(s.rootPath, makeClusterTopologyLatestVersionKey(clusterID))

	// Check whether the latest version is equal to that in etcd. If it is equal，update cluster topology and latest version; Otherwise, return an error.
	cmp := clientv3.Compare(clientv3.Value(latestVersionKey), "=", fmtID(latestVersion))
	opPutClusterTopology := clientv3.OpPut(key, string(value))
	opPutLatestVersion := clientv3.OpPut(latestVersionKey, fmtID(clusterTopology.DataVersion))

	resp, err := s.Txn(ctx).
		If(cmp).
		Then(opPutClusterTopology, opPutLatestVersion).
		Commit()
	if err != nil {
		return errors.Wrapf(err, "fail to put cluster topology, clusterID:%d, key:%s", clusterID, key)
	}
	if !resp.Succeeded {
		return ErrParsePutClusterTopology.WithCausef("cluster topology may have been modified, clusterID:%d, resp:%v", clusterID, resp)
	}

	return nil
}

func (s *metaStorageImpl) ListSchemas(ctx context.Context, clusterID uint32) ([]*clusterpb.Schema, error) {
	schemas := make([]*clusterpb.Schema, 0)
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
			return nil, errors.Wrapf(err, "fail to list schemas, clusterID:%d, start key:%s, end key:%s, range limit:%d", clusterID, startKey, endKey, rangeLimit)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		for _, r := range res {
			schema := &clusterpb.Schema{}
			if err := proto.Unmarshal([]byte(r), schema); err != nil {
				return nil, ErrParseListSchemas.WithCausef("fail to parse proto, clusterID:%d, err:%v", clusterID, err)
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

func (s *metaStorageImpl) CreateSchema(ctx context.Context, clusterID uint32, schema *clusterpb.Schema) (*clusterpb.Schema, error) {
	now := time.Now()
	schema.CreatedAt = uint64(now.Unix())

	value, err := proto.Marshal(schema)
	if err != nil {
		return nil, ErrParseCreateSchema.WithCausef("fail to parse proto, clusterID:%d, schemaID:%d, err:%v", clusterID, schema.Id, err)
	}

	key := path.Join(s.rootPath, makeSchemaKey(clusterID, schema.Id))

	// Check if the key exists, if not，create schema; Otherwise, the schema already exists and return an error.
	cmp := clientv3util.KeyMissing(key)
	opCreateSchema := clientv3.OpPut(key, string(value))

	resp, err := s.Txn(ctx).
		If(cmp).
		Then(opCreateSchema).
		Commit()
	if err != nil {
		return nil, errors.Wrapf(err, "fail to create schema, clusterID:%d, schemaID:%d, key:%s", clusterID, schema.Id, key)
	}
	if !resp.Succeeded {
		return nil, ErrParsePutCluster.WithCausef("schema may already exist, clusterID:%d, schemaID:%d, resp:%v", clusterID, schema.Id, resp)
	}
	return schema, nil
}

// TODO: operator in a batch
func (s *metaStorageImpl) PutSchemas(ctx context.Context, clusterID uint32, schemas []*clusterpb.Schema) error {
	for _, schema := range schemas {
		key := makeSchemaKey(clusterID, schema.Id)
		value, err := proto.Marshal(schema)
		if err != nil {
			return ErrParsePutSchemas.WithCausef("fail to parse proto, clusterID:%d, schemaID:%d, err:%v", clusterID, schema.Id, err)
		}

		if err = s.Put(ctx, key, string(value)); err != nil {
			return errors.Wrapf(err, "fail to put schemas, clusterID:%d, schemaID:%d, key:%s", clusterID, schema.Id, key)
		}
	}
	return nil
}

func (s *metaStorageImpl) CreateTable(ctx context.Context, clusterID uint32, schemaID uint32, table *clusterpb.Table) (*clusterpb.Table, error) {
	now := time.Now()
	table.CreatedAt = uint64(now.Unix())

	value, err := proto.Marshal(table)
	if err != nil {
		return nil, ErrParseCreateTable.WithCausef("fail to parse proto, clusterID:%d, schemaID:%d, tableID:%d, err:%v", clusterID, schemaID, table.Id, err)
	}

	key := path.Join(s.rootPath, makeTableKey(clusterID, schemaID, table.Id))
	nameToIDKey := path.Join(s.rootPath, makeNameToIDKey(clusterID, schemaID, table.Name))

	// Check if the key and the name to id key exists, if not，create table; Otherwise, the table already exists and return an error.
	cmp := clientv3util.KeyMissing(key)
	cmpNameToID := clientv3util.KeyMissing(nameToIDKey)
	opCreateTable := clientv3.OpPut(key, string(value))
	opCreateNameToID := clientv3.OpPut(nameToIDKey, fmtID(table.Id))

	resp, err := s.Txn(ctx).
		If(cmp, cmpNameToID).
		Then(opCreateTable, opCreateNameToID).
		Commit()
	if err != nil {
		return nil, errors.Wrapf(err, "fail to create table, clusterID:%d, schemaID:%d, tableID:%d, key:%s", clusterID, schemaID, table.Id, key)
	}
	if !resp.Succeeded {
		return nil, ErrParseCreateTable.WithCausef("table may already exist, clusterID:%d, schemaID:%d, tableID:%d, resp:%v", clusterID, schemaID, table.Id, resp)
	}
	return table, nil
}

func (s *metaStorageImpl) GetTable(ctx context.Context, clusterID uint32, schemaID uint32, tableName string) (*clusterpb.Table, bool, error) {
	value, err := s.Get(ctx, makeNameToIDKey(clusterID, schemaID, tableName))
	if err != nil {
		return nil, false, errors.Wrapf(err, "fail to get table id, clusterID:%d, schemaID:%d, table name:%s", clusterID, schemaID, tableName)
	}
	if value == "" {
		return nil, false, nil
	}
	tableID, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return nil, false, errors.Wrapf(err, "string to int failed")
	}

	key := makeTableKey(clusterID, schemaID, tableID)
	value, err = s.Get(ctx, key)
	if err != nil {
		return nil, false, errors.Wrapf(err, "fail to get table, clusterID:%d, schemaID:%d, tableID:%d, key:%s", clusterID, schemaID, tableID, key)
	}

	table := &clusterpb.Table{}
	if err = proto.Unmarshal([]byte(value), table); err != nil {
		return nil, false, ErrParseGetTable.WithCausef("fail to parse proto, clusterID:%d, schemaID:%d, tableID:%d, err:%v", clusterID, schemaID, tableID, err)
	}

	return table, true, nil
}

func (s *metaStorageImpl) ListTables(ctx context.Context, clusterID uint32, schemaID uint32) ([]*clusterpb.Table, error) {
	tables := make([]*clusterpb.Table, 0)
	nextID := uint64(0)
	endKey := makeTableKey(clusterID, schemaID, math.MaxUint64)

	rangeLimit := s.opts.MaxScanLimit
	for {
		startKey := makeTableKey(clusterID, schemaID, nextID)
		_, res, err := s.Scan(ctx, startKey, endKey, rangeLimit)
		if err != nil {
			if rangeLimit /= 2; rangeLimit >= s.opts.MinScanLimit {
				continue
			}
			return nil, errors.Wrapf(err, "fail to list tables, clusterID:%d, schemaID:%d, start key:%s, end key:%s, range limit:%d", clusterID, schemaID, startKey, endKey, rangeLimit)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		for _, r := range res {
			table := &clusterpb.Table{}
			if err := proto.Unmarshal([]byte(r), table); err != nil {
				return nil, ErrParseListTables.WithCausef("fail to parse proto, clusterID:%d, schemaID:%d, err:%v", clusterID, schemaID, err)
			}

			tables = append(tables, table)
			if table.GetId() == math.MaxUint64 {
				log.Warn("list table table_id has reached max value", zap.Uint64("table-id", table.GetId()))
				return tables, nil
			}
			nextID = table.GetId() + 1
		}

		if len(res) < rangeLimit {
			return tables, nil
		}
	}
}

// TODO: operator in a batch
func (s *metaStorageImpl) PutTables(ctx context.Context, clusterID uint32, schemaID uint32, tables []*clusterpb.Table) error {
	for _, table := range tables {
		key := makeTableKey(clusterID, schemaID, table.Id)
		value, err := proto.Marshal(table)
		if err != nil {
			return ErrParsePutTables.WithCausef("fail to parse proto, clusterID:%d, schemaID:%d, tableID:%d, err:%v", clusterID, schemaID, table.Id, err)
		}

		if err = s.Put(ctx, key, string(value)); err != nil {
			return errors.Wrapf(err, "fail to put tables, clusterID:%d, schemaID:%d, tableID:%d, key:%s", clusterID, schemaID, table.Id, key)
		}
	}
	return nil
}

func (s *metaStorageImpl) DeleteTable(ctx context.Context, clusterID uint32, schemaID uint32, tableName string) error {
	value, err := s.Get(ctx, makeNameToIDKey(clusterID, schemaID, tableName))
	if err != nil {
		return errors.Wrapf(err, "fail to get table id, clusterID:%d, schemaID:%d, table name:%s", clusterID, schemaID, tableName)
	}
	if value == "" {
		return ErrParseDeleteTable.WithCausef("table not found, clusterID:%d, schemaID:%d, table name:%s", clusterID, schemaID, tableName)
	}
	tableID, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return errors.Wrapf(err, "string to int failed")
	}

	key := makeTableKey(clusterID, schemaID, tableID)
	if err := s.Delete(ctx, key); err != nil {
		return errors.Wrapf(err, "fail to delete table, clusterID:%d, schemaID:%d, tableName:%s, tableID:%d, key:%s", clusterID, schemaID, tableName, tableID, key)
	}
	if err := s.Delete(ctx, makeNameToIDKey(clusterID, schemaID, tableName)); err != nil {
		return errors.Wrapf(err, "fail to delete table id, clusterID:%d, schemaID:%d, table name:%s", clusterID, schemaID, tableName)
	}
	return nil
}

// TODO: opertor in a batch
func (s *metaStorageImpl) CreateShardTopologies(ctx context.Context, clusterID uint32, shardTopologies []*clusterpb.ShardTopology) ([]*clusterpb.ShardTopology, error) {
	now := time.Now()
	for _, shardTopology := range shardTopologies {
		shardTopology.CreatedAt = uint64(now.Unix())

		value, err := proto.Marshal(shardTopology)
		if err != nil {
			return nil, ErrParseCreateShardTopology.WithCausef("fail to parse proto, clusterID:%d, shardID:%d, err:%v", clusterID, shardTopology.ShardId, err)
		}

		key := path.Join(s.rootPath, makeShardTopologyKey(clusterID, shardTopology.GetShardId(), fmtID(shardTopology.Version)))
		latestVersionKey := path.Join(s.rootPath, makeShardLatestVersionKey(clusterID, shardTopology.GetShardId()))

		// Check if the key and latest version key exists, if not，create shard topology and latest version; Otherwise, the shard topology already exists and return an error.
		cmpLatestVersionKey := clientv3util.KeyMissing(latestVersionKey)
		cmpKey := clientv3util.KeyMissing(key)
		opCreateShardTopology := clientv3.OpPut(key, string(value))
		opCreateShardTopologyLatestVersion := clientv3.OpPut(latestVersionKey, fmtID(shardTopology.Version))

		resp, err := s.Txn(ctx).
			If(cmpLatestVersionKey, cmpKey).
			Then(opCreateShardTopology, opCreateShardTopologyLatestVersion).
			Commit()
		if err != nil {
			return nil, errors.Wrapf(err, "fail to create shard topology, clusterID:%d, shardID:%d, key:%s", clusterID, shardTopology.ShardId, key)
		}
		if !resp.Succeeded {
			return nil, ErrParseCreateClusterTopology.WithCausef("shard topology may already exist, clusterID:%d, shardID:%d, resp:%v", clusterID, shardTopology.ShardId, resp)
		}
	}
	return shardTopologies, nil
}

// TODO: operator in a batch
func (s *metaStorageImpl) ListShardTopologies(ctx context.Context, clusterID uint32, shardIDs []uint32) ([]*clusterpb.ShardTopology, error) {
	shardTopologies := make([]*clusterpb.ShardTopology, 0)

	for _, shardID := range shardIDs {
		key := makeShardLatestVersionKey(clusterID, shardID)
		version, err := s.Get(ctx, key)
		if err != nil {
			return nil, errors.Wrapf(err, "fail to list shard topology latest version, clusterID:%d, shardID:%d, key:%s", clusterID, shardID, key)
		}

		key = makeShardTopologyKey(clusterID, shardID, version)
		value, err := s.Get(ctx, key)
		if err != nil {
			return nil, errors.Wrapf(err, "fail to list shard topology, clusterID:%d, shardID:%d, key:%s", clusterID, shardID, key)
		}

		shardTopology := &clusterpb.ShardTopology{}
		if err = proto.Unmarshal([]byte(value), shardTopology); err != nil {
			return nil, ErrParseListShardTopology.WithCausef("fail to parse proto, clusterID:%d, shardID:%d, err:%v", clusterID, shardID, err)
		}
		shardTopologies = append(shardTopologies, shardTopology)
	}
	return shardTopologies, nil
}

// TODO: operator in a batch
func (s *metaStorageImpl) PutShardTopologies(ctx context.Context, clusterID uint32, shardIDs []uint32, latestVersion uint64, shardTopologies []*clusterpb.ShardTopology) error {
	for index, shardID := range shardIDs {
		value, err := proto.Marshal(shardTopologies[index])
		if err != nil {
			return ErrParsePutShardTopology.WithCausef("fail to parse proto, clusterID:%d, shardID:%d, err:%v", clusterID, shardID, err)
		}

		key := path.Join(s.rootPath, makeShardTopologyKey(clusterID, shardID, fmtID(shardTopologies[index].Version)))
		latestVersionKey := path.Join(s.rootPath, makeShardLatestVersionKey(clusterID, shardID))

		// Check whether the latest version is equal to that in etcd. If it is equal，update shard topology and latest version; Otherwise, return an error.
		cmp := clientv3.Compare(clientv3.Value(latestVersionKey), "=", fmtID(latestVersion))
		opPutLatestVersion := clientv3.OpPut(latestVersionKey, fmtID(shardTopologies[index].Version))
		opPutShardTopology := clientv3.OpPut(key, string(value))

		resp, err := s.Txn(ctx).
			If(cmp).
			Then(opPutLatestVersion, opPutShardTopology).
			Commit()
		if err != nil {
			return errors.Wrapf(err, "fail to put shard topology, clusterID:%d, shardID:%d, key:%s", clusterID, shardID, key)
		}
		if !resp.Succeeded {
			return ErrParsePutShardTopology.WithCausef("shard topology may have been modified, clusterID:%d, shardID:%d, resp:%v", clusterID, shardID, resp)
		}
	}
	return nil
}

func (s *metaStorageImpl) ListNodes(ctx context.Context, clusterID uint32) ([]*clusterpb.Node, error) {
	defaultStartNodeName := string([]byte{0})
	defaultEndNodeName := string([]byte{255})
	startKey := makeNodeKey(clusterID, defaultStartNodeName)
	endKey := makeNodeKey(clusterID, defaultEndNodeName)

	nodes := make([]*clusterpb.Node, 0)

	rangeLimit := s.opts.MaxScanLimit
	for {
		keys, res, err := s.Scan(ctx, startKey, endKey, rangeLimit)
		if err != nil {
			if rangeLimit /= 2; rangeLimit >= s.opts.MinScanLimit {
				continue
			}
			return nil, errors.Wrapf(err, "fail to list nodes, clusterID:%d, start key:%s, end key:%s, range limit:%d", clusterID, startKey, endKey, rangeLimit)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if len(nodes) > 0 {
			nodes = nodes[:len(nodes)-1]
		}

		for index, r := range res {
			node := &clusterpb.Node{}
			if err := proto.Unmarshal([]byte(r), node); err != nil {
				return nil, ErrParseListNodes.WithCausef("fail to parse proto, clusterID:%d, err:%v", clusterID, err)
			}

			nodes = append(nodes, node)

			startKey = keys[index]
		}

		if len(res) < rangeLimit {
			return nodes, nil
		}
	}
}

func (s *metaStorageImpl) PutNodes(ctx context.Context, clusterID uint32, nodes []*clusterpb.Node) error {
	return nil
}

func (s *metaStorageImpl) CreateOrUpdateNode(ctx context.Context, clusterID uint32, node *clusterpb.Node) (*clusterpb.Node, error) {
	now := time.Now()
	node.LastTouchTime = uint64(now.Unix())

	CreateNode := node
	CreateNode.CreateTime = CreateNode.LastTouchTime
	UpdateNode := node

	key := path.Join(s.rootPath, makeNodeKey(clusterID, node.Name))
	CreateNodeValue, err := proto.Marshal(CreateNode)
	if err != nil {
		return nil, ErrParseCreateOrUpdateNode.WithCausef("fail to parse proto, clusterID:%d, node name:%s, err:%v", clusterID, node.Name, err)
	}
	UpdateNodeValue, err := proto.Marshal(UpdateNode)
	if err != nil {
		return nil, ErrParseCreateOrUpdateNode.WithCausef("fail to parse proto, clusterID:%d, node name:%s, err:%v", clusterID, node.Name, err)
	}

	// Check if the key exists, if not，create node; Otherwise, update node.
	cmp := clientv3util.KeyMissing(key)
	opCreateNode := clientv3.OpPut(key, string(CreateNodeValue))
	opUpdateNode := clientv3.OpPut(key, string(UpdateNodeValue))

	_, err = s.Txn(ctx).
		If(cmp).
		Then(opCreateNode).
		Else(opUpdateNode).
		Commit()
	if err != nil {
		return nil, errors.Wrapf(err, "fail to create or update node, clusterID:%d, node name:%s, key:%s", clusterID, node.Name, key)
	}

	return node, nil
}
