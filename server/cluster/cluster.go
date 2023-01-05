// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"context"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/id"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type Cluster struct {
	clusterID storage.ClusterID

	// RWMutex is used to protect following fields.
	// TODO: Encapsulated maps as a specific struct.
	lock     sync.RWMutex
	metaData storage.Cluster

	tableManager    TableManager
	topologyManager TopologyManager

	// Manage the registered nodes from heartbeat.
	registeredNodesCache map[string]RegisteredNode // nodeName -> NodeName

	storage      storage.Storage
	kv           clientv3.KV
	shardIDAlloc id.Allocator
}

func NewCluster(meta storage.Cluster, storage storage.Storage, kv clientv3.KV, rootPath string, idAllocatorStep uint) *Cluster {
	schemaIDAlloc := id.NewAllocatorImpl(kv, path.Join(rootPath, meta.Name, AllocSchemaIDPrefix), idAllocatorStep)
	tableIDAlloc := id.NewAllocatorImpl(kv, path.Join(rootPath, meta.Name, AllocTableIDPrefix), idAllocatorStep)
	// FIXME: Load ShardTopology when cluster create, pass exist ShardID to allocator.
	shardIDAlloc := id.NewReusableAllocatorImpl([]uint64{}, MinShardID)

	cluster := &Cluster{
		clusterID:            meta.ID,
		metaData:             meta,
		tableManager:         NewTableManagerImpl(storage, meta.ID, schemaIDAlloc, tableIDAlloc),
		topologyManager:      NewTopologyManagerImpl(storage, meta.ID, shardIDAlloc),
		registeredNodesCache: map[string]RegisteredNode{},
		storage:              storage,
		kv:                   kv,
		shardIDAlloc:         shardIDAlloc,
	}

	return cluster
}

func (c *Cluster) GetClusterID() storage.ClusterID {
	return c.clusterID
}

func (c *Cluster) Name() string {
	return c.metaData.Name
}

func (c *Cluster) GetShardTables(shardIDs []storage.ShardID, nodeName string) map[storage.ShardID]ShardTables {
	shardTableIDs := c.topologyManager.GetTableIDs(shardIDs, nodeName)

	result := make(map[storage.ShardID]ShardTables, len(shardIDs))

	schemas := c.tableManager.GetSchemas()
	schemaByID := make(map[storage.SchemaID]storage.Schema)
	for _, schema := range schemas {
		schemaByID[schema.ID] = schema
	}

	for shardID, shardTableID := range shardTableIDs {
		tables := c.tableManager.GetTablesByIDs(shardTableID.TableIDs)
		tableInfos := make([]TableInfo, 0, len(tables))
		for _, table := range tables {
			schema, ok := schemaByID[table.SchemaID]
			if !ok {
				log.Warn("schema not exits", zap.Uint64("schemaID", uint64(table.SchemaID)))
			}
			tableInfos = append(tableInfos, TableInfo{
				ID:         table.ID,
				Name:       table.Name,
				SchemaID:   table.SchemaID,
				SchemaName: schema.Name,
			})
		}
		result[shardID] = ShardTables{
			Shard: ShardInfo{
				ID:      shardTableID.ShardNode.ID,
				Role:    shardTableID.ShardNode.ShardRole,
				Version: shardTableID.Version,
			},
			Tables: tableInfos,
		}
	}

	for _, shardID := range shardIDs {
		_, exists := result[shardID]
		if !exists {
			result[shardID] = ShardTables{}
		}
	}
	return result
}

// DropTable will drop table metadata and all mapping of this table.
// If the table to be dropped has been opened multiple times, all its mapping will be dropped.
func (c *Cluster) DropTable(ctx context.Context, schemaName, tableName string) (DropTableResult, error) {
	log.Info("drop table start", zap.String("cluster", c.Name()), zap.String("schemaName", schemaName), zap.String("tableName", tableName))

	table, ok, err := c.tableManager.GetTable(schemaName, tableName)
	if err != nil {
		return DropTableResult{}, errors.WithMessage(err, "get table")
	}

	if !ok {
		return DropTableResult{}, ErrTableNotFound
	}

	// Drop table.
	err = c.tableManager.DropTable(ctx, schemaName, tableName)
	if err != nil {
		return DropTableResult{}, errors.WithMessagef(err, "table manager drop table")
	}

	// Remove dropped table in shard view.
	updateVersions, err := c.topologyManager.RemoveTable(ctx, table.ID)
	if err != nil {
		return DropTableResult{}, errors.WithMessagef(err, "topology manager remove table")
	}

	ret := DropTableResult{
		ShardVersionUpdate: updateVersions,
	}
	log.Info("drop table success", zap.String("cluster", c.Name()), zap.String("schemaName", schemaName), zap.String("tableName", tableName), zap.String("result", fmt.Sprintf("%+v", ret)))
	return ret, nil
}

func (c *Cluster) updateShardTables(ctx context.Context, shardTablesArr []ShardTables) error {
	for _, shardTables := range shardTablesArr {
		tableIDs := make([]storage.TableID, 0, len(shardTables.Tables))
		for _, table := range shardTables.Tables {
			tableIDs = append(tableIDs, table.ID)
		}

		_, err := c.topologyManager.UpdateShardView(ctx, storage.ShardView{
			ShardID:   shardTables.Shard.ID,
			Version:   shardTables.Shard.Version + 1,
			TableIDs:  tableIDs,
			CreatedAt: uint64(time.Now().UnixMilli()),
		})
		if err != nil {
			return errors.WithMessagef(err, "update shard tables")
		}
	}

	return nil
}

// OpenTable will open an existing table on the specified shard.
// The table to be opened must have been created.
func (c *Cluster) OpenTable(ctx context.Context, request OpenTableRequest) error {
	table, _, err := c.GetTable(request.SchemaName, request.TableName)
	if err != nil {
		log.Error("get table", zap.Error(err), zap.String("schemaName", request.SchemaName), zap.String("tableName", request.TableName))
		return err
	}

	shardTables, exists := c.GetShardTables([]storage.ShardID{request.ShardID}, request.NodeName)[request.ShardID]
	if !exists {
		log.Error("get shard tables", zap.Error(ErrShardNotFound), zap.Uint64("shardID", uint64(request.ShardID)), zap.String("nodeName", request.NodeName))
		return errors.WithMessage(ErrShardNotFound, fmt.Sprintf("shard tables not found, shardID:%d, nodeName:%s", request.ShardID, request.NodeName))
	}

	shardTables.Tables = append(shardTables.Tables, TableInfo{
		table.ID,
		table.Name,
		table.SchemaID,
		request.SchemaName,
		table.Partitioned,
	})

	if err := c.updateShardTables(ctx, []ShardTables{shardTables}); err != nil {
		log.Error("update shard tables", zap.Error(err))
		return err
	}

	return nil
}

func (c *Cluster) CloseTable(ctx context.Context, request CloseTableRequest) error {
	table, _, err := c.GetTable(request.SchemaName, request.TableName)
	if err != nil {
		log.Error("get table", zap.Error(err), zap.String("schemaName", request.SchemaName), zap.String("tableName", request.TableName))
		return err
	}

	shardTables, exists := c.GetShardTables([]storage.ShardID{request.ShardID}, request.NodeName)[request.ShardID]
	if !exists {
		log.Error("get shard tables", zap.Error(ErrShardNotFound), zap.Uint64("shardID", uint64(request.ShardID)), zap.String("nodeName", request.NodeName))
		return errors.WithMessage(ErrShardNotFound, fmt.Sprintf("shard tables not found, shardID:%d, nodeName:%s", request.ShardID, request.NodeName))
	}

	found := false
	for i, tableInfo := range shardTables.Tables {
		if tableInfo.ID == table.ID {
			found = true
			shardTables.Tables = append(shardTables.Tables[:i], shardTables.Tables[i+1:]...)
			break
		}
	}
	if !found {
		return errors.WithMessage(ErrTableNotFound, fmt.Sprintf("table not found on shard, shardID:%d, tableID:%d, tableName:%s", request.ShardID, table.ID, table.Name))
	}

	if err := c.updateShardTables(ctx, []ShardTables{shardTables}); err != nil {
		log.Error("update shard tables", zap.Error(err))
		return err
	}

	return nil
}

// MigrateTable used to migrate tables from old shard to new shard.
// The mapping relationship between table and shard will be modified.
func (c *Cluster) MigrateTable(ctx context.Context, request MigrateTableRequest) error {
	originShardTables := c.GetShardTables([]storage.ShardID{request.OldShardID}, request.NodeName)[request.OldShardID]

	// Find remaining tables in old shard.
	var remainingTables []TableInfo

	for _, tableInfo := range originShardTables.Tables {
		found := false
		for _, tableName := range request.TableNames {
			if tableInfo.Name == tableName && tableInfo.SchemaName == request.SchemaName {
				found = true
				break
			}
		}
		if !found {
			remainingTables = append(remainingTables, tableInfo)
		}
	}

	// Update shard tables.
	originShardTables.Tables = remainingTables

	getNodeShardsResult, err := c.GetNodeShards(ctx)
	if err != nil {
		log.Error("get node shards", zap.Error(err))
		return err
	}

	// Find new shard in metadata.
	var newShardInfo ShardInfo
	found := false
	for _, shardNodeWithVersion := range getNodeShardsResult.NodeShards {
		if shardNodeWithVersion.ShardInfo.ID == request.NewShardID {
			newShardInfo = shardNodeWithVersion.ShardInfo
			found = true
			break
		}
	}
	if !found {
		log.Error("new shard not found", zap.Error(ErrShardNotFound), zap.Uint64("shardID", uint64(request.NewShardID)))
		return errors.WithMessage(ErrShardNotFound, fmt.Sprintf("new shard not found, shardID:%d", request.NewShardID))
	}

	// Find split tables in metadata.
	var tables []TableInfo
	for _, tableName := range request.TableNames {
		table, exists, err := c.GetTable(request.SchemaName, tableName)
		if err != nil {
			log.Error("get table", zap.Error(err), zap.String("schemaName", request.SchemaName), zap.String("tableName", tableName))
			return err
		}
		if !exists {
			log.Error("table not found", zap.Error(err), zap.String("schemaName", request.SchemaName), zap.String("tableName", tableName))
			return errors.WithMessage(ErrTableNotFound, fmt.Sprintf("table not found, schemaName:%s, tableName:%s", request.SchemaName, tableName))
		}
		tables = append(tables, TableInfo{
			ID:         table.ID,
			Name:       table.Name,
			SchemaID:   table.SchemaID,
			SchemaName: request.SchemaName,
		})
	}
	newShardTables := ShardTables{
		Shard:  newShardInfo,
		Tables: tables,
	}
	if err := c.updateShardTables(ctx, []ShardTables{originShardTables, newShardTables}); err != nil {
		log.Error("update shard tables", zap.Error(err))
		return err
	}

	return nil
}

// GetOrCreateSchema the second output parameter bool: returns true if the schema was newly created.
func (c *Cluster) GetOrCreateSchema(ctx context.Context, schemaName string) (storage.Schema, bool, error) {
	return c.tableManager.GetOrCreateSchema(ctx, schemaName)
}

// GetTable the second output parameter bool: returns true if the table exists.
func (c *Cluster) GetTable(schemaName, tableName string) (storage.Table, bool, error) {
	return c.tableManager.GetTable(schemaName, tableName)
}

func (c *Cluster) CreateTable(ctx context.Context, nodeName string, schemaName string, tableName string, partitioned bool) (CreateTableResult, error) {
	log.Info("create table start", zap.String("cluster", c.Name()), zap.String("nodeName", nodeName), zap.String("schemaName", schemaName), zap.String("tableName", tableName))

	_, exists, err := c.tableManager.GetTable(schemaName, tableName)
	if err != nil {
		return CreateTableResult{}, err
	}

	if exists {
		return CreateTableResult{}, ErrTableAlreadyExists
	}

	// Create table in table manager.
	table, err := c.tableManager.CreateTable(ctx, schemaName, tableName, partitioned)
	if err != nil {
		return CreateTableResult{}, errors.WithMessage(err, "table manager create table")
	}

	// Add table to topology manager.
	result, err := c.topologyManager.AddTable(ctx, nodeName, table)
	if err != nil {
		return CreateTableResult{}, errors.WithMessage(err, "topology manager add table")
	}

	ret := CreateTableResult{
		Table:              table,
		ShardVersionUpdate: result,
	}
	log.Info("create table succeed", zap.String("cluster", c.Name()), zap.String("result", fmt.Sprintf("%+v", ret)))
	return ret, nil
}

func (c *Cluster) GetShardNodesByShardID(id storage.ShardID) ([]storage.ShardNode, error) {
	return c.topologyManager.GetShardNodesByID(id)
}

func (c *Cluster) GetShardNodeByTableIDs(tableIDs []storage.TableID) (GetShardNodesByTableIDsResult, error) {
	return c.topologyManager.GetShardNodesByTableIDs(tableIDs)
}

func (c *Cluster) RegisterNode(ctx context.Context, registeredNode RegisteredNode) error {
	registeredNode.Node.State = storage.NodeStateOnline
	err := c.storage.CreateOrUpdateNode(ctx, storage.CreateOrUpdateNodeRequest{
		ClusterID: c.clusterID,
		Node:      registeredNode.Node,
	})
	if err != nil {
		return errors.WithMessage(err, "create or update registered node")
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	c.registeredNodesCache[registeredNode.Node.Name] = registeredNode

	return nil
}

func (c *Cluster) GetRegisteredNodes() []RegisteredNode {
	c.lock.RLock()
	defer c.lock.RUnlock()

	nodes := make([]RegisteredNode, 0, len(c.registeredNodesCache))
	for _, node := range c.registeredNodesCache {
		nodes = append(nodes, node)
	}
	return nodes
}

func (c *Cluster) GetRegisteredNodeByName(nodeName string) (RegisteredNode, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	registeredNode, ok := c.registeredNodesCache[nodeName]
	return registeredNode, ok
}

func (c *Cluster) AllocShardID(ctx context.Context) (uint32, error) {
	id, err := c.shardIDAlloc.Alloc(ctx)
	if err != nil {
		return 0, errors.WithMessage(err, "alloc shard id")
	}
	return uint32(id), nil
}

func (c *Cluster) RouteTables(_ context.Context, schemaName string, tableNames []string) (RouteTablesResult, error) {
	tables := make(map[storage.TableID]storage.Table, len(tableNames))
	tableIDs := make([]storage.TableID, 0, len(tableNames))
	for _, tableName := range tableNames {
		table, exists, err := c.tableManager.GetTable(schemaName, tableName)
		if err != nil {
			return RouteTablesResult{}, errors.WithMessage(err, "table manager get table")
		}
		if exists {
			tables[table.ID] = table
			tableIDs = append(tableIDs, table.ID)
		}
	}

	tableShardNodesWithShardViewVersion, err := c.topologyManager.GetShardNodesByTableIDs(tableIDs)
	if err != nil {
		return RouteTablesResult{}, errors.WithMessage(err, "topology get shard nodes by table ids")
	}
	routeEntries := make(map[string]RouteEntry, len(tableNames))
	for tableID, value := range tableShardNodesWithShardViewVersion.ShardNodes {
		nodeShards := make([]ShardNodeWithVersion, 0, len(value))
		for _, shardNode := range value {
			nodeShards = append(nodeShards, ShardNodeWithVersion{
				ShardInfo: ShardInfo{
					ID:      shardNode.ID,
					Role:    shardNode.ShardRole,
					Version: tableShardNodesWithShardViewVersion.Version[shardNode.ID],
				},
				ShardNode: shardNode,
			})
		}
		table := tables[tableID]
		routeEntries[table.Name] = RouteEntry{
			Table: TableInfo{
				ID:         table.ID,
				Name:       table.Name,
				SchemaID:   table.SchemaID,
				SchemaName: schemaName,
			},
			NodeShards: nodeShards,
		}
	}
	return RouteTablesResult{
		ClusterViewVersion: c.topologyManager.GetVersion(),
		RouteEntries:       routeEntries,
	}, nil
}

func (c *Cluster) GetNodeShards(_ context.Context) (GetNodeShardsResult, error) {
	getNodeShardsResult := c.topologyManager.GetShardNodes()

	shardNodesWithVersion := make([]ShardNodeWithVersion, 0, len(getNodeShardsResult.shardNodes))

	for _, shardNode := range getNodeShardsResult.shardNodes {
		shardNodesWithVersion = append(shardNodesWithVersion, ShardNodeWithVersion{
			ShardInfo: ShardInfo{
				ID:      shardNode.ID,
				Role:    shardNode.ShardRole,
				Version: getNodeShardsResult.versions[shardNode.ID],
			},
			ShardNode: shardNode,
		})
	}

	return GetNodeShardsResult{
		ClusterTopologyVersion: c.topologyManager.GetVersion(),
		NodeShards:             shardNodesWithVersion,
	}, nil
}

func (c *Cluster) GetClusterViewVersion() uint64 {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.topologyManager.GetVersion()
}

func (c *Cluster) GetClusterMinNodeCount() uint32 {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.metaData.MinNodeCount
}

func (c *Cluster) GetTotalShardNum() uint32 {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.metaData.ShardTotal
}

func (c *Cluster) GetClusterState() storage.ClusterState {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.topologyManager.GetClusterState()
}

func (c *Cluster) UpdateClusterView(ctx context.Context, state storage.ClusterState, shardNodes []storage.ShardNode) error {
	if err := c.topologyManager.UpdateClusterView(ctx, state, shardNodes); err != nil {
		return errors.WithMessage(err, "update cluster view")
	}
	return nil
}

func (c *Cluster) CreateShardViews(ctx context.Context, views []CreateShardView) error {
	if err := c.topologyManager.CreateShardViews(ctx, views); err != nil {
		return errors.WithMessage(err, "topology manager create shard views")
	}

	return nil
}

// Initialize the cluster view and shard view of the cluster.
// It will be used when we create the cluster.
func (c *Cluster) init(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.topologyManager.InitClusterView(ctx)
}

// Load cluster NodeName from storage into memory.
func (c *Cluster) load(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if err := c.tableManager.Load(ctx); err != nil {
		return errors.WithMessage(err, "load table manager")
	}

	if err := c.topologyManager.Load(ctx); err != nil {
		return errors.WithMessage(err, "load topology manager")
	}

	return nil
}
