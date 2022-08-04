// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import (
	"context"
	"math"
	"path"
	"strconv"
	"time"

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
			return nil, errors.Wrapf(err, "meta storage list clusters failed, start key:%s, end key:%s, range limit:%d", startKey, endKey, rangeLimit)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		for _, r := range res {
			cluster := &clusterpb.Cluster{}
			if err := proto.Unmarshal([]byte(r), cluster); err != nil {
				return nil, ErrParseListClusters.WithCausef("proto parse failed, err:%v", err)
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
		return nil, ErrParseCreateCluster.WithCausef("proto parse failed, err:%v", err)
	}

	key := path.Join(s.rootPath, makeClusterKey(cluster.Id))

	cmp := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	opCreateCluster := clientv3.OpPut(key, string(value))

	resp, err := s.Txn(ctx).
		If(cmp).
		Then(opCreateCluster).
		Commit()
	if err != nil {
		return nil, errors.Wrapf(err, "meta storage create cluster failed, clusterID:%d, key:%s", cluster.Id, key)
	}
	if !resp.Succeeded {
		return nil, ErrParseCreateCluster.WithCausef("resp:%v", resp)
	}
	return cluster, nil
}

func (s *metaStorageImpl) GetCluster(ctx context.Context, clusterID uint32) (*clusterpb.Cluster, error) {
	key := makeClusterKey(clusterID)
	value, err := s.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrapf(err, "meta storage get cluster failed, clusterID:%d, key:%s", clusterID, key)
	}

	meta := &clusterpb.Cluster{}
	if err = proto.Unmarshal([]byte(value), meta); err != nil {
		return nil, ErrParseGetCluster.WithCausef("proto parse failed, clusterID:%d, err:%v", clusterID, err)
	}
	return meta, nil
}

func (s *metaStorageImpl) PutCluster(ctx context.Context, clusterID uint32, meta *clusterpb.Cluster) error {
	value, err := proto.Marshal(meta)
	if err != nil {
		return ErrParsePutCluster.WithCausef("proto parse failed, clusterID:%d, err:%v", clusterID, err)
	}

	key := makeClusterKey(clusterID)
	err = s.Put(ctx, key, string(value))
	if err != nil {
		return errors.Wrapf(err, "meta storage put cluster failed, clusterID:%d, key:%s", clusterID, key)
	}
	return nil
}

func (s *metaStorageImpl) CreateClusterTopology(ctx context.Context, clusterTopology *clusterpb.ClusterTopology) (*clusterpb.ClusterTopology, error) {
	now := time.Now()
	clusterTopology.CreatedAt = uint64(now.Unix())

	value, err := proto.Marshal(clusterTopology)
	if err != nil {
		return nil, ErrParseCreateClusterTopology.WithCausef("proto parse failed, err:%v", err)
	}

	key := path.Join(s.rootPath, makeClusterTopologyKey(clusterTopology.ClusterId, fmtID(clusterTopology.DataVersion)))
	latestVersionKey := path.Join(s.rootPath, makeClusterTopologyLatestVersionKey(clusterTopology.ClusterId))

	cmplatestVersionKey := clientv3.Compare(clientv3.CreateRevision(latestVersionKey), "=", 0)
	cmpKey := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	opCreateClsuterTopology := clientv3.OpPut(key, string(value))
	opCreateClsuterTopologyLatestVersion := clientv3.OpPut(latestVersionKey, fmtID(clusterTopology.DataVersion))

	resp, err := s.Txn(ctx).
		If(cmplatestVersionKey, cmpKey).
		Then(opCreateClsuterTopology, opCreateClsuterTopologyLatestVersion).
		Commit()
	if err != nil {
		return nil, errors.Wrapf(err, "meta storage create cluster topology failed, clusterID:%d, key:%s", clusterTopology.ClusterId, key)
	}
	if !resp.Succeeded {
		return nil, ErrParseCreateClusterTopology.WithCausef("resp:%v", resp)
	}
	return clusterTopology, nil
}

func (s *metaStorageImpl) GetClusterTopology(ctx context.Context, clusterID uint32) (*clusterpb.ClusterTopology, error) {
	key := makeClusterTopologyLatestVersionKey(clusterID)
	version, err := s.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrapf(err, "meta storage get cluster topology latest version failed, clusterID:%d, key:%s", clusterID, key)
	}

	key = makeClusterTopologyKey(clusterID, version)
	value, err := s.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrapf(err, "meta storage get cluster topology failed, clusterID:%d, key:%s", clusterID, key)
	}

	clusterMetaData := &clusterpb.ClusterTopology{}
	if err = proto.Unmarshal([]byte(value), clusterMetaData); err != nil {
		return nil, ErrParseGetClusterTopology.WithCausef("proto parse failed, clusterID:%d, err:%v", clusterID, err)
	}
	return clusterMetaData, nil
}

func (s *metaStorageImpl) PutClusterTopology(ctx context.Context, clusterID uint32, latestVersion uint64, clusterMetaData *clusterpb.ClusterTopology) error {
	value, err := proto.Marshal(clusterMetaData)
	if err != nil {
		return ErrParsePutClusterTopology.WithCausef("proto parse failed, clusterID:%d, err:%v", clusterID, err)
	}

	key := path.Join(s.rootPath, makeClusterTopologyKey(clusterID, fmtID(clusterMetaData.DataVersion)))
	latestVersionKey := path.Join(s.rootPath, makeClusterTopologyLatestVersionKey(clusterID))

	cmp := clientv3.Compare(clientv3.Value(latestVersionKey), "=", fmtID(latestVersion))
	opPutClusterTopology := clientv3.OpPut(key, string(value))
	opPutLatestVersion := clientv3.OpPut(latestVersionKey, fmtID(clusterMetaData.DataVersion))

	resp, err := s.Txn(ctx).
		If(cmp).
		Then(opPutClusterTopology, opPutLatestVersion).
		Commit()
	if err != nil {
		return errors.Wrapf(err, "meta storage put cluster topology failed, clusterID:%d, key:%s", clusterID, key)
	}
	if !resp.Succeeded {
		return ErrParsePutClusterTopology.WithCausef("clusterID:%d, resp:%v", clusterID, resp)
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
			return nil, errors.Wrapf(err, "meta storage list schemas failed, clusterID:%d, start key:%s, end key:%s, range limit:%d", clusterID, startKey, endKey, rangeLimit)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		for _, r := range res {
			schema := &clusterpb.Schema{}
			if err := proto.Unmarshal([]byte(r), schema); err != nil {
				return nil, ErrParseListSchemas.WithCausef("proto parse failed, clusterID:%d, err:%v", clusterID, err)
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
		return nil, ErrParseCreateSchema.WithCausef("proto parse failed, err:%v", err)
	}

	key := path.Join(s.rootPath, makeSchemaKey(clusterID, schema.Id))

	cmp := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	opCreateSchema := clientv3.OpPut(key, string(value))

	resp, err := s.Txn(ctx).
		If(cmp).
		Then(opCreateSchema).
		Commit()
	if err != nil {
		return nil, errors.Wrapf(err, "meta storage create schema failed, clusterID:%d, schemaID:%d, key:%s", clusterID, schema.Id, key)
	}
	if !resp.Succeeded {
		return nil, ErrParsePutCluster.WithCausef("resp:%v", resp)
	}
	return schema, nil
}

// TODO: operator in a batch
func (s *metaStorageImpl) PutSchemas(ctx context.Context, clusterID uint32, schemas []*clusterpb.Schema) error {
	for _, schema := range schemas {
		key := makeSchemaKey(clusterID, schema.Id)
		value, err := proto.Marshal(schema)
		if err != nil {
			return ErrParsePutSchemas.WithCausef("proto parse failed, clusterID:%d, schemaID:%d, err:%v", clusterID, schema.Id, err)
		}

		if err = s.Put(ctx, key, string(value)); err != nil {
			return errors.Wrapf(err, "meta storage put schemas failed, clusterID:%d, schemaID:%d, key:%s", clusterID, schema.Id, key)
		}
	}
	return nil
}

func (s *metaStorageImpl) CreateTable(ctx context.Context, clusterID uint32, schemaID uint32, table *clusterpb.Table) (*clusterpb.Table, error) {
	now := time.Now()
	table.CreatedAt = uint64(now.Unix())

	value, err := proto.Marshal(table)
	if err != nil {
		return nil, ErrParseCreateTable.WithCausef("proto parse failed, err:%v", err)
	}

	key := path.Join(s.rootPath, makeTableKey(clusterID, schemaID, table.Id))
	nameToIDKey := path.Join(s.rootPath, table.Name)

	cmp := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	cmpNameToID := clientv3.Compare(clientv3.CreateRevision(nameToIDKey), "=", 0)
	opCreateTable := clientv3.OpPut(key, string(value))
	opCreateNameToID := clientv3.OpPut(nameToIDKey, fmtID(table.Id))

	resp, err := s.Txn(ctx).
		If(cmp, cmpNameToID).
		Then(opCreateTable, opCreateNameToID).
		Commit()
	if err != nil {
		return nil, errors.Wrapf(err, "meta storage create table failed, clusterID:%d, schemaID:%d, tableID:%d, key:%s", clusterID, schemaID, table.Id, key)
	}
	if !resp.Succeeded {
		return nil, ErrParseCreateTable.WithCausef("resp:%v", resp)
	}
	return table, nil
}

func (s *metaStorageImpl) GetTable(ctx context.Context, clusterID uint32, schemaID uint32, tableName string) (*clusterpb.Table, bool, error) {
	value, err := s.Get(ctx, tableName)
	if err != nil {
		return nil, false, errors.Wrapf(err, "meta storage get table id failed, table name:%s", tableName)
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
		return nil, false, errors.Wrapf(err, "meta storage get table failed, clusterID:%d, schemaID:%d, tableID:%d, key:%s", clusterID, schemaID, tableID, key)
	}

	table := &clusterpb.Table{}
	if err = proto.Unmarshal([]byte(value), table); err != nil {
		return nil, false, ErrParseGetTable.WithCausef("proto parse failed, err:%v", err)
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
			return nil, errors.Wrapf(err, "meta storage list tables failed, clusterID:%d, schemaID:%d, start key:%s, end key:%s, range limit:%d", clusterID, schemaID, startKey, endKey, rangeLimit)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		for _, r := range res {
			table := &clusterpb.Table{}
			if err := proto.Unmarshal([]byte(r), table); err != nil {
				return nil, ErrParseListTables.WithCausef("proto parse failed, clusterID:%d, schemaID:%d, err:%v", clusterID, schemaID, err)
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
	for _, item := range tables {
		key := makeTableKey(clusterID, schemaID, item.Id)
		value, err := proto.Marshal(item)
		if err != nil {
			return ErrParsePutTables.WithCausef("proto parse failed, clusterID:%d, schemaID:%d, tableID:%d, err:%v", clusterID, schemaID, item.Id, err)
		}

		if err = s.Put(ctx, key, string(value)); err != nil {
			return errors.Wrapf(err, "meta storage put tables failed, clusterID:%d, schemaID:%d, tableID:%d, key:%s", clusterID, schemaID, item.Id, key)
		}
	}
	return nil
}

func (s *metaStorageImpl) DeleteTable(ctx context.Context, clusterID uint32, schemaID uint32, tableName string) error {
	value, err := s.Get(ctx, tableName)
	if err != nil {
		return errors.Wrapf(err, "meta storage get table id failed, table name:%s", tableName)
	}
	tableID, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return errors.Wrapf(err, "string to int failed")
	}

	key := makeTableKey(clusterID, schemaID, tableID)
	if err := s.Delete(ctx, key); err != nil {
		return errors.Wrapf(err, "meta storage delete table failed, clusterID:%d, schemaID:%d, tableID:%d, key:%s", clusterID, schemaID, tableID, key)
	}
	if err := s.Delete(ctx, tableName); err != nil {
		return errors.Wrapf(err, "meta storage delete table id failed, tableID:%d, tableName:%s", tableID, tableName)
	}
	return nil
}

func (s *metaStorageImpl) CreateShardTopologies(ctx context.Context, clusterID uint32, shardTopologies []*clusterpb.ShardTopology) ([]*clusterpb.ShardTopology, error) {
	now := time.Now()
	for _, shardTopology := range shardTopologies {
		shardTopology.CreatedAt = uint64(now.Unix())

		value, err := proto.Marshal(shardTopology)
		if err != nil {
			return nil, ErrParseCreateShardTopology.WithCausef("proto parse failed, err:%v", err)
		}

		key := path.Join(s.rootPath, makeShardTopologyKey(clusterID, shardTopology.GetShardId(), fmtID(shardTopology.Version)))
		latestVersionKey := path.Join(s.rootPath, makeShardLatestVersionKey(clusterID, shardTopology.GetShardId()))

		cmplatestVersionKey := clientv3.Compare(clientv3.CreateRevision(latestVersionKey), "=", 0)
		cmpKey := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
		opCreateClsuterTopology := clientv3.OpPut(key, string(value))
		opCreateClsuterTopologyLatestVersion := clientv3.OpPut(latestVersionKey, fmtID(shardTopology.Version))

		resp, err := s.Txn(ctx).
			If(cmplatestVersionKey, cmpKey).
			Then(opCreateClsuterTopology, opCreateClsuterTopologyLatestVersion).
			Commit()
		if err != nil {
			return nil, errors.Wrapf(err, "meta storage create cluster topology failed, clusterID:%d, key:%s", clusterID, key)
		}
		if !resp.Succeeded {
			return nil, ErrParseCreateClusterTopology.WithCausef("resp:%v", resp)
		}
	}
	return shardTopologies, nil
}

// TODO: operator in a batch
func (s *metaStorageImpl) ListShardTopologies(ctx context.Context, clusterID uint32, shardIDs []uint32) ([]*clusterpb.ShardTopology, error) {
	shardTableInfo := make([]*clusterpb.ShardTopology, 0)

	for _, shardID := range shardIDs {
		key := makeShardLatestVersionKey(clusterID, shardID)
		version, err := s.Get(ctx, key)
		if err != nil {
			return nil, errors.Wrapf(err, "meta storage list shard topology latest version failed, clusterID:%d, shardID:%d, key:%s", clusterID, shardID, key)
		}

		key = makeShardTopologyKey(clusterID, shardID, version)
		value, err := s.Get(ctx, key)
		if err != nil {
			return nil, errors.Wrapf(err, "meta storage list shard topology failed, clusterID:%d, shardID:%d, key:%s", clusterID, shardID, key)
		}

		shardTopology := &clusterpb.ShardTopology{}
		if err = proto.Unmarshal([]byte(value), shardTopology); err != nil {
			return nil, ErrParseListShardTopology.WithCausef("proto parse failed, clusterID:%d, shardID:%d, err:%v", clusterID, shardID, err)
		}
		shardTableInfo = append(shardTableInfo, shardTopology)
	}
	return shardTableInfo, nil
}

// TODO: operator in a batch
func (s *metaStorageImpl) PutShardTopologies(ctx context.Context, clusterID uint32, shardIDs []uint32, latestVersion uint64, shardTableInfo []*clusterpb.ShardTopology) error {
	for index, shardID := range shardIDs {
		value, err := proto.Marshal(shardTableInfo[index])
		if err != nil {
			return ErrParsePutShardTopology.WithCausef("proto parse failed, clusterID:%d, shardID:%d, err:%v", clusterID, shardID, err)
		}

		key := path.Join(s.rootPath, makeShardTopologyKey(clusterID, shardID, fmtID(shardTableInfo[index].Version)))
		latestVersionKey := path.Join(s.rootPath, makeShardLatestVersionKey(clusterID, shardID))

		cmp := clientv3.Compare(clientv3.Value(latestVersionKey), "=", fmtID(latestVersion))
		opPutLatestVersion := clientv3.OpPut(latestVersionKey, fmtID(shardTableInfo[index].Version))
		opPutShardTopology := clientv3.OpPut(key, string(value))

		resp, err := s.Txn(ctx).
			If(cmp).
			Then(opPutLatestVersion, opPutShardTopology).
			Commit()
		if err != nil {
			return errors.Wrapf(err, "meta storage put shard failed, clusterID:%d, shardID:%d, key:%s", clusterID, shardID, key)
		}
		if !resp.Succeeded {
			return ErrParsePutShardTopology.WithCausef("clusterID:%d, shardID:%d, resp:%v", clusterID, shardID, resp)
		}
	}
	return nil
}

func (s *metaStorageImpl) ListNodes(ctx context.Context, clusterID uint32) ([]*clusterpb.Node, error) {
	nodes := make([]*clusterpb.Node, 0)
	startKey := makeNodeKey(clusterID, "0.0.0.0:8081")
	endKey := makeNodeKey(clusterID, "255.255.255.255:8081")

	rangeLimit := s.opts.MaxScanLimit
	for {
		keys, res, err := s.Scan(ctx, startKey, endKey, rangeLimit)
		if err != nil {
			if rangeLimit /= 2; rangeLimit >= s.opts.MinScanLimit {
				continue
			}
			return nil, errors.Wrapf(err, "meta storage list nodes failed, clusterID:%d, start key:%s, end key:%s, range limit:%d", clusterID, startKey, endKey, rangeLimit)
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
				return nil, ErrParseListNodes.WithCausef("proto parse failed, clusterID:%d, err:%s", clusterID, err)
			}

			nodes = append(nodes, node)
			if node.Name == "255.255.255.255:8081" {
				log.Warn("list node node_name has reached max value: 255.255.255.255:8081")
				return nodes, nil
			}
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
	value, err := proto.Marshal(CreateNode)
	if err != nil {
		return nil, ErrParseCreateOrUpdateNode.WithCausef("proto parse failed, clusterID:%d, node name:%s, err:%v", clusterID, node.Name, err)
	}
	UpdateNodevalue, err := proto.Marshal(UpdateNode)
	if err != nil {
		return nil, ErrParseCreateOrUpdateNode.WithCausef("proto parse failed, clusterID:%d, node name:%s, err:%v", clusterID, node.Name, err)
	}

	cmp := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	opCreateNode := clientv3.OpPut(key, string(value))
	opUpdateNode := clientv3.OpPut(key, string(UpdateNodevalue))

	_, err = s.Txn(ctx).
		If(cmp).
		Then(opCreateNode).
		Else(opUpdateNode).
		Commit()
	if err != nil {
		return nil, errors.Wrapf(err, "meta storage create node failed, clusterID:%d, node name:%s, key:%s", clusterID, node.Name, key)
	}

	return node, nil
}
