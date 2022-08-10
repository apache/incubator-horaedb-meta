// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"context"
	"crypto/rand"
	"math/big"
	"sync"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/server/id"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
)

type Cluster struct {
	clusterID uint32

	// RWMutex is used to project following fields.
	lock         sync.RWMutex
	metaData     *metaData
	shardsCache  map[uint32]*Shard  // shard_id -> shard
	schemasCache map[string]*Schema // schema_name -> schema
	nodesCache   map[string]*Node   // node_name -> node

	storage     storage.Storage
	coordinator *coordinator
	alloc       id.Allocator
}

func NewCluster(cluster *clusterpb.Cluster, storage storage.Storage) *Cluster {
	alloc := id.NewAllocatorImpl(storage, "/aaa", cluster.Name+"/alloc-id")
	return &Cluster{
		clusterID:    cluster.GetId(),
		storage:      storage,
		alloc:        alloc,
		metaData:     &metaData{cluster: cluster},
		shardsCache:  make(map[uint32]*Shard),
		schemasCache: make(map[string]*Schema),
		nodesCache:   make(map[string]*Node),
	}
}

func (c *Cluster) Name() string {
	return c.metaData.cluster.Name
}

func (c *Cluster) init(ctx context.Context, shards []*clusterpb.Shard) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.metaData.clusterTopology.ShardView = shards
	c.metaData.clusterTopology.State = clusterpb.ClusterTopology_STABLE
	if err := c.storage.PutClusterTopology(ctx, c.clusterID, c.metaData.clusterTopology.DataVersion, c.metaData.clusterTopology); err != nil {
		return errors.Wrap(err, "cluster init")
	}

	shardTopologies := make([]*clusterpb.ShardTopology, 0, c.metaData.cluster.ShardTotal)

	for i := uint32(0); i < c.metaData.cluster.ShardTotal; i++ {
		shardTopologies = append(shardTopologies, &clusterpb.ShardTopology{
			ShardId:  i,
			TableIds: make([]uint64, 0),
		})
	}

	if _, err := c.storage.CreateShardTopologies(ctx, c.clusterID, shardTopologies); err != nil {
		return errors.Wrap(err, "cluster init")
	}
	return nil
}

func (c *Cluster) Load(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.coordinator = newCoordinator(c)

	shards, shardIDs, err := c.loadClusterTopologyLocked(ctx)
	if err != nil {
		return errors.Wrap(err, "clusters Load")
	}

	shardTopologies, err := c.loadShardTopologyLocked(ctx, shardIDs)
	if err != nil {
		return errors.Wrap(err, "clusters Load")
	}

	schemas, err := c.loadSchemaLocked(ctx)
	if err != nil {
		return errors.Wrap(err, "clusters Load")
	}

	nodes, err := c.loadNodeLocked(ctx)
	if err != nil {
		return errors.Wrap(err, "clusters Load")
	}

	tables, err := c.loadTableLocked(ctx, schemas)
	if err != nil {
		return errors.Wrap(err, "clusters Load")
	}

	if err := c.updateCacheLocked(shards, shardTopologies, schemas, nodes, tables); err != nil {
		return errors.Wrap(err, "clusters Load")
	}
	return nil
}

func (c *Cluster) updateCacheLocked(
	shards map[uint32][]*clusterpb.Shard,
	shardTopologies map[uint32]*clusterpb.ShardTopology,
	schemasLoaded map[string]*clusterpb.Schema,
	nodesLoaded map[string]*clusterpb.Node,
	tablesLoaded map[string]map[uint64]*clusterpb.Table,
) error {
	// update schemas cache
	for schemaName, tables := range tablesLoaded {
		for _, table := range tables {
			_, ok := c.schemasCache[schemaName]
			if ok {
				c.schemasCache[schemaName].tableMap[table.GetName()] = &Table{
					schema: schemasLoaded[schemaName],
					meta:   table,
				}
			} else {
				c.schemasCache[schemaName] = &Schema{
					meta: schemasLoaded[schemaName],
					tableMap: map[string]*Table{table.GetName(): {
						schema: schemasLoaded[schemaName],
						meta:   table,
					}},
				}
			}
		}
	}

	// update node cache
	for shardID, shardPbs := range shards {
		for _, shard := range shardPbs {
			if _, ok := c.nodesCache[shard.GetNode()]; ok {
				c.nodesCache[shard.GetNode()].shardIDs = append(c.nodesCache[shard.GetNode()].shardIDs, shardID)
			} else {
				// TODO: check node not found by node name
				c.nodesCache[shard.GetNode()] = &Node{meta: nodesLoaded[shard.GetNode()], shardIDs: []uint32{shardID}}
			}
		}
	}

	// update shards cache
	for shardID, shardTopology := range shardTopologies {
		tables := make(map[uint64]*Table, len(shardTopology.TableIds))

		for _, tableID := range shardTopology.TableIds {
			for schemaName, tableMap := range tablesLoaded {
				table, ok := tableMap[tableID]
				if ok {
					tables[tableID] = &Table{
						schema: schemasLoaded[schemaName],
						meta:   table,
					}
				}
			}
		}
		// TODO: assert shardID
		// TODO: check shard not found by shardID
		shardMetaList := shards[shardID]
		var nodes []*clusterpb.Node
		for _, shardMeta := range shardMetaList {
			if node := c.nodesCache[shardMeta.Node]; node != nil {
				nodes = append(nodes, node.meta)
			}
		}
		c.shardsCache[shardID] = &Shard{
			meta:    shards[shardID],
			nodes:   nodes,
			tables:  tables,
			version: 0,
		}
	}

	return nil
}

func (c *Cluster) updateSchemaCacheLocked(schemaPb *clusterpb.Schema) *Schema {
	schema := &Schema{meta: schemaPb, tableMap: make(map[string]*Table, 0)}
	c.schemasCache[schemaPb.GetName()] = schema
	return schema
}

func (c *Cluster) updateTableCacheLocked(shardID uint32, schema *Schema, tablePb *clusterpb.Table) *Table {
	table := &Table{meta: tablePb, schema: schema.meta, shardID: shardID}
	schema.tableMap[tablePb.GetName()] = table
	c.shardsCache[tablePb.GetShardId()].tables[table.GetID()] = table
	return table
}

func (c *Cluster) getTableShardIDLocked(tableID uint64) (uint32, error) {
	for id, shard := range c.shardsCache {
		if _, ok := shard.tables[tableID]; ok {
			return id, nil
		}
	}
	return 0, ErrShardNotFound.WithCausef("get table shardID, tableID:%d", tableID)
}

func (c *Cluster) GetTables(ctx context.Context, shardIDs []uint32, nodeName string) (map[uint32]*ShardTablesWithRole, error) {
	// TODO: refactor more fine-grained locks
	c.lock.RLock()
	defer c.lock.RUnlock()

	shardTables := make(map[uint32]*ShardTablesWithRole, len(shardIDs))
	for _, shardID := range shardIDs {
		shard, ok := c.shardsCache[shardID]
		if !ok {
			return nil, ErrShardNotFound.WithCausef("shardID:%d", shardID)
		}

		shardRole := clusterpb.ShardRole_FOLLOWER
		found := false
		for i, n := range shard.nodes {
			if nodeName == n.GetName() {
				found = true
				shardRole = shard.meta[i].ShardRole
				break
			}
		}
		if !found {
			return nil, ErrNodeNotFound.WithCausef("nodeName not found in current shard, shardID:%d, nodeName:%s", shardID, nodeName)
		}

		tables := make([]*Table, 0, len(shard.tables))
		for _, table := range shard.tables {
			tables = append(tables, table)
		}
		shardTables[shardID] = &ShardTablesWithRole{shardRole: shardRole, tables: tables, version: shard.version}
	}

	return shardTables, nil
}

func (c *Cluster) DropTable(ctx context.Context, schemaName, tableName string, tableID uint64) error {
	schema, exists := c.getSchemaLocked(schemaName)
	if !exists {
		return ErrSchemaNotFound.WithCausef("schemaName:%s", schemaName)
	}

	if err := c.storage.DeleteTable(ctx, c.clusterID, schema.GetID(), tableName); err != nil {
		return errors.Wrapf(err, "clusters DropTable, clusterID:%d, schema:%v, tableName:%s",
			c.clusterID, schema, tableName)
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	schema.dropTableLocked(tableName)
	for _, shard := range c.shardsCache {
		shard.dropTableLocked(tableID)
	}
	return nil
}

func (c *Cluster) CreateSchema(ctx context.Context, schemaName string) (*Schema, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Check if provided schema exists.
	schema, exists := c.getSchemaLocked(schemaName)
	if exists {
		return schema, nil
	}

	// alloc schema id
	schemaID, err := c.allocSchemaID(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "cluster AllocSchemaID, "+
			"schemaName:%s", schemaName)
	}

	// Save schema in storage.
	schemaPb := &clusterpb.Schema{Id: schemaID, Name: schemaName, ClusterId: c.clusterID}
	schemaPb, err = c.storage.CreateSchema(ctx, c.clusterID, schemaPb)
	if err != nil {
		return nil, errors.Wrap(err, "clusters CreateSchema")
	}

	// Update schemasCache in memory.
	schema = c.updateSchemaCacheLocked(schemaPb)
	return schema, nil
}

func (c *Cluster) CreateTable(ctx context.Context, shardID uint32, schemaName string, tableName string) (*Table, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Check provided schema if exists.
	schema, exists := c.getSchemaLocked(schemaName)
	if !exists {
		return nil, ErrSchemaNotFound.WithCausef("schemaName", schemaName)
	}

	// check if exists
	table, exists := c.getTableLocked(schemaName, tableName)
	if exists {
		return table, nil
	}

	// alloc table id
	tableID, err := c.allocTableID(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "cluster AllocTableID, "+
			"schemaName:%s, tableName:%s", schemaName, tableName)
	}

	// Save table in storage.
	tablePb := &clusterpb.Table{Id: tableID, Name: tableName, SchemaId: schema.GetID(), ShardId: shardID}
	tablePb, err = c.storage.CreateTable(ctx, c.clusterID, schema.GetID(), tablePb)
	if err != nil {
		return nil, errors.Wrap(err, "clusters CreateTable")
	}

	// Update shardTopology in storage.
	shardTopologies, err := c.storage.ListShardTopologies(ctx, c.clusterID, []uint32{shardID})
	if len(shardTopologies) != 1 {
		return nil, errors.Wrapf(err, "clusters CreateTable, shard has more than one shardTopology, shardID:%d, shardTopologies:%v",
			shardID, shardTopologies)
	}
	shardTopologies[0].TableIds = append(shardTopologies[0].TableIds, tableID)
	if err = c.storage.PutShardTopologies(ctx, c.clusterID, []uint32{shardID}, shardTopologies[0].GetVersion(), shardTopologies); err != nil {
		return nil, errors.Wrap(err, "clusters CreateTable")
	}

	// Update tableCache in memory.
	table = c.updateTableCacheLocked(shardID, schema, tablePb)
	return table, nil
}

func (c *Cluster) loadClusterTopologyLocked(ctx context.Context) (map[uint32][]*clusterpb.Shard, []uint32, error) {
	clusterTopology, err := c.storage.GetClusterTopology(ctx, c.clusterID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "clusters loadClusterTopologyLocked")
	}
	c.metaData.clusterTopology = clusterTopology

	if c.metaData.clusterTopology == nil {
		return nil, nil, ErrClusterTopologyNotFound.WithCausef("clusters:%v", c)
	}

	shardMap := make(map[uint32][]*clusterpb.Shard)
	for _, shard := range c.metaData.clusterTopology.ShardView {
		shardMap[shard.Id] = append(shardMap[shard.Id], shard)
	}

	shardIDs := make([]uint32, 0, len(shardMap))
	for id := range shardMap {
		shardIDs = append(shardIDs, id)
	}

	return shardMap, shardIDs, nil
}

func (c *Cluster) loadShardTopologyLocked(ctx context.Context, shardIDs []uint32) (map[uint32]*clusterpb.ShardTopology, error) {
	topologies, err := c.storage.ListShardTopologies(ctx, c.clusterID, shardIDs)
	if err != nil {
		return nil, errors.Wrap(err, "clusters loadShardTopologyLocked")
	}
	shardTopologyMap := make(map[uint32]*clusterpb.ShardTopology, len(shardIDs))
	for i, topology := range topologies {
		shardTopologyMap[shardIDs[i]] = topology
	}
	return shardTopologyMap, nil
}

func (c *Cluster) loadSchemaLocked(ctx context.Context) (map[string]*clusterpb.Schema, error) {
	schemas, err := c.storage.ListSchemas(ctx, c.clusterID)
	if err != nil {
		return nil, errors.Wrap(err, "clusters loadSchemaLocked")
	}
	schemaMap := make(map[string]*clusterpb.Schema, len(schemas))
	for _, schema := range schemas {
		schemaMap[schema.Name] = schema
	}
	return schemaMap, nil
}

func (c *Cluster) loadNodeLocked(ctx context.Context) (map[string]*clusterpb.Node, error) {
	nodes, err := c.storage.ListNodes(ctx, c.clusterID)
	if err != nil {
		return nil, errors.Wrap(err, "clusters loadNodeLocked")
	}
	nodeMap := make(map[string]*clusterpb.Node, len(nodes))
	for _, node := range nodes {
		nodeMap[node.Name] = node
	}
	return nodeMap, nil
}

func (c *Cluster) loadTableLocked(ctx context.Context, schemas map[string]*clusterpb.Schema) (map[string]map[uint64]*clusterpb.Table, error) {
	tables := make(map[string]map[uint64]*clusterpb.Table)
	for _, schema := range schemas {
		tablePbs, err := c.storage.ListTables(ctx, c.clusterID, schema.Id)
		if err != nil {
			return nil, errors.Wrap(err, "clusters loadTableLocked")
		}
		for _, table := range tablePbs {
			if t, ok := tables[schema.GetName()]; ok {
				t[table.GetId()] = table
			} else {
				tables[schema.GetName()] = map[uint64]*clusterpb.Table{table.GetId(): table}
			}
		}
	}
	return tables, nil
}

func (c *Cluster) getSchemaLocked(schemaName string) (*Schema, bool) {
	schema, ok := c.schemasCache[schemaName]
	return schema, ok
}

func (c *Cluster) getTableLocked(schemaName string, tableName string) (*Table, bool) {
	table, ok := c.schemasCache[schemaName].tableMap[tableName]
	return table, ok
}

func (c *Cluster) GetTable(ctx context.Context, schemaName, tableName string) (*Table, bool, error) {
	c.lock.RLock()
	schema, ok := c.schemasCache[schemaName]
	if !ok {
		c.lock.RUnlock()
		return nil, false, ErrSchemaNotFound.WithCausef("schemaName:%s", schemaName)
	}

	table, exists := schema.getTable(tableName)
	if exists {
		c.lock.RUnlock()
		return table, true, nil
	}
	c.lock.RUnlock()

	// Search Table in storage.
	tablePb, exists, err := c.storage.GetTable(ctx, c.clusterID, schema.GetID(), tableName)
	if err != nil {
		return nil, false, errors.Wrap(err, "clusters GetTable")
	}
	if exists {
		c.lock.Lock()
		defer c.lock.Unlock()

		shardID, err := c.getTableShardIDLocked(tablePb.GetId())
		if err != nil {
			return nil, false, errors.Wrap(err, "clusters GetTable")
		}
		table = c.updateTableCacheLocked(shardID, schema, tablePb)
		return table, true, nil
	}

	return nil, false, nil
}

func (c *Cluster) GetShardIDs(nodeName string) ([]uint32, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	node, ok := c.nodesCache[nodeName]
	if !ok {
		return nil, ErrNodeNotFound.WithCausef("clusters GetShardIDs, nodeName:%s", nodeName)
	}
	return node.shardIDs, nil
}

func (c *Cluster) RegisterNode(ctx context.Context, nodeName string, lease uint32) error {
	nodePb := &clusterpb.Node{NodeStats: &clusterpb.NodeStats{Lease: lease}, Name: nodeName}
	nodePb1, err := c.storage.CreateOrUpdateNode(ctx, c.clusterID, nodePb)
	if err != nil {
		return errors.Wrapf(err, "clusters manager RegisterNode, nodeName:%s", nodeName)
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	node, ok := c.nodesCache[nodeName]
	if ok {
		node.meta = nodePb1
	} else {
		c.nodesCache[nodeName] = &Node{meta: nodePb1}
	}
	return nil
}

func (c *Cluster) allocSchemaID(ctx context.Context) (uint32, error) {
	ID, err := c.alloc.Alloc(ctx)
	if err != nil {
		return 0, errors.Wrapf(err, "alloc schema id failed")
	}
	return uint32(ID), nil
}

func (c *Cluster) allocTableID(ctx context.Context) (uint64, error) {
	ID, err := c.alloc.Alloc(ctx)
	if err != nil {
		return 0, errors.Wrapf(err, "alloc table id failed")
	}
	return ID, nil
}

func (c *Cluster) assignShardID(nodeName string) (uint32, error) {
	if node, ok := c.nodesCache[nodeName]; ok {
		if len(node.shardIDs) == 0 {
			return 0, ErrNodeShardsIsEmpty.WithCausef("nodeName:%s", nodeName)
		}
		id, err := rand.Int(rand.Reader, big.NewInt(int64(len(node.shardIDs))))
		if err != nil {
			return 0, errors.Wrapf(err, "assign shard id failed, nodeName:%s", nodeName)
		}
		return node.shardIDs[uint32(id.Uint64())], nil
	}
	return 0, ErrNodeNotFound.WithCausef("nodeName:%s", nodeName)
}

type metaData struct {
	cluster         *clusterpb.Cluster
	clusterTopology *clusterpb.ClusterTopology
}
