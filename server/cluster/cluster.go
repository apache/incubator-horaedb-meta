package cluster

import (
	"context"
	"sync"

	"github.com/CeresDB/ceresdbproto/pkg/metapb"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
)

type Cluster struct {
	sync.RWMutex
	clusterID uint32

	storage storage.Storage

	metaData *MetaData

	shards  map[uint32]*Shard
	schemas map[string]*Schema
	nodes   map[string][]uint32

	coordinator *coordinator
}

func NewCluster(clusterID uint32, storage storage.Storage) *Cluster {
	return &Cluster{clusterID: clusterID, storage: storage, metaData: &MetaData{}, shards: make(map[uint32]*Shard), schemas: make(map[string]*Schema), nodes: make(map[string][]uint32)}
}

func (c *Cluster) Name() string {
	return c.metaData.cluster.Name
}

func (c *Cluster) Load(ctx context.Context) error {
	c.Lock()
	defer c.Unlock()

	if err := c.loadClusterLocked(ctx); err != nil {
		return errors.Wrap(err, "cluster Load")
	}

	if err := c.loadClusterTopologyLocked(ctx); err != nil {
		return errors.Wrap(err, "cluster Load")
	}

	if err := c.loadShardTopologyLocked(ctx); err != nil {
		return errors.Wrap(err, "cluster Load")
	}

	if err := c.loadSchemaLocked(ctx); err != nil {
		return errors.Wrap(err, "cluster Load")
	}

	if err := c.loadTableLocked(ctx); err != nil {
		return errors.Wrap(err, "cluster Load")
	}

	if err := c.updateCacheLocked(ctx); err != nil {
		return errors.Wrap(err, "cluster Load")
	}
	return nil
}

func (c *Cluster) updateCacheLocked(ctx context.Context) error {
	for schemaName, tables := range c.metaData.tableMap {
		for _, table := range tables {
			_, ok := c.schemas[schemaName]
			if ok {
				c.schemas[schemaName].tableMap[table.GetName()] = &Table{
					schema: c.metaData.schemaMap[schemaName],
					meta:   table,
				}
			} else {
				c.schemas[schemaName] = &Schema{
					meta: c.metaData.schemaMap[schemaName],
					tableMap: map[string]*Table{table.GetName(): {
						schema: c.metaData.schemaMap[schemaName],
						meta:   table,
					}},
				}
			}
		}
	}

	for shardID, shardTopology := range c.metaData.shardTopologyMap {
		var tables []*Table
		for _, tableID := range shardTopology.TableIds {
			for schemaName, tableMap := range c.metaData.tableMap {
				table, ok := tableMap[tableID]
				if ok {
					tables = append(tables, &Table{
						schema: c.metaData.schemaMap[schemaName],
						meta:   table,
					})
				}
			}
		}
		// todo: assert shardID
		// todo: check shard not found by shardID
		shardMetaList := c.metaData.shardMap[shardID]
		var nodes []*metapb.Node
		for _, shardMeta := range shardMetaList {
			nodes = append(nodes, c.metaData.nodeMap[shardMeta.Node])
		}
		c.shards[shardID] = &Shard{
			meta:    c.metaData.shardMap[shardID],
			nodes:   nodes,
			tables:  tables,
			version: 0,
		}
	}

	for shardID, shards := range c.metaData.shardMap {
		for _, shard := range shards {
			if _, ok := c.nodes[shard.GetNode()]; ok {
				c.nodes[shard.GetNode()] = append(c.nodes[shard.GetNode()], shardID)
			} else {
				c.nodes[shard.GetNode()] = []uint32{shardID}
			}
		}
	}

	return nil
}

func (c *Cluster) updateSchemaCacheLocked(schemaPb *metapb.Schema) *Schema {
	schema := &Schema{meta: schemaPb}
	c.schemas[schemaPb.GetName()] = schema
	return schema
}

func (c *Cluster) updateTableCacheLocked(schema *Schema, tablePb *metapb.Table) *Table {
	table := &Table{meta: tablePb, schema: schema.meta}
	schema.tableMap[tablePb.GetName()] = table
	c.shards[tablePb.GetShardId()].tables = append(c.shards[tablePb.GetShardId()].tables, table)
	return table
}

func (c *Cluster) GetTables(ctx context.Context, shardIDs []uint32, node string) (map[uint32]*ShardTablesWithRole, error) {
	shardTables := make(map[uint32]*ShardTablesWithRole, len(shardIDs))
	for _, shardID := range shardIDs {
		shardTable, ok := c.shards[shardID]
		if !ok {
			return nil, ErrShardNotFound.WithCausef("shard_id", shardID)
		}

		shardRole := FOLLOWER
		for i, n := range shardTable.nodes {
			if node == n.GetNode() {
				if shardTable.meta[i].ShardRole == metapb.ShardRole_LEADER {
					shardRole = LEADER
				} else {
					shardRole = FOLLOWER
				}
				break
			}
		}
		shardTables[shardID] = &ShardTablesWithRole{shardRole: shardRole, tables: shardTable.tables, version: shardTable.version}
	}

	return shardTables, nil
}

func (c *Cluster) DropTable(ctx context.Context, schemaName, tableName string, tableID uint64) error {
	return nil
}

func (c *Cluster) CreateSchema(ctx context.Context, schemaName string, schemaID uint32) (*Schema, error) {
	c.Lock()
	defer c.Unlock()
	// check if exists
	{
		schema, exists := c.getSchema(ctx, schemaName)
		if exists {
			return schema, nil
		}
	}

	// persist
	schemaPb := &metapb.Schema{Id: schemaID, Name: schemaName, ClusterId: c.clusterID}
	err := c.storage.CreateSchema(ctx, c.clusterID, schemaPb)
	if err != nil {
		return nil, errors.Wrap(err, "CreateSchema")
	}

	// cache
	schema := c.updateSchemaCacheLocked(schemaPb)
	return schema, nil
}

func (c *Cluster) CreateTable(ctx context.Context, schemaName string, shardID uint32, tableName string, tableID uint64) (*Table, error) {
	c.Lock()
	defer c.Unlock()
	// check if exists
	schema, exists := c.getSchema(schemaName)
	if !exists {
		return nil, ErrSchemaNotFound.WithCausef("schema_name", schemaName)
	}

	// persist
	tablePb := &metapb.Table{Id: tableID, Name: tableName, SchemaId: schema.GetID(), ShardId: shardID}
	err1 := c.storage.CreateTable(ctx, c.clusterID, schema.GetID(), tablePb)
	if err1 != nil {
		return nil, errors.Wrap(err1, "CreateTable")
	}

	// update cache
	table := c.updateTableCacheLocked(schema, tablePb)
	return table, nil
}

func (c *Cluster) loadClusterLocked(ctx context.Context) error {
	cluster, err := c.storage.GetCluster(ctx, c.clusterID)
	if err != nil {
		return err
	}
	c.metaData.cluster = cluster
	return nil
}

func (c *Cluster) loadClusterTopologyLocked(ctx context.Context) error {
	clusterTopology, err := c.storage.GetClusterTopology(ctx, c.clusterID)
	if err != nil {
		return err
	}
	c.metaData.clusterTopology = clusterTopology

	if c.metaData.clusterTopology == nil {
		return ErrClusterTopologyNotFound
	}
	shardIDs := make([]uint32, len(c.metaData.clusterTopology.ShardView))
	shardMap := make(map[uint32][]*metapb.Shard)

	for _, shard := range c.metaData.clusterTopology.ShardView {
		shardMap[shard.Id] = append(shardMap[shard.Id], shard)
		shardIDs = append(shardIDs, shard.Id)
	}
	c.metaData.shardMap = shardMap
	c.metaData.shardIDs = shardIDs
	return nil
}

func (c *Cluster) loadShardTopologyLocked(ctx context.Context) error {
	topologies, err := c.storage.ListShardTopologies(ctx, c.clusterID, c.metaData.shardIDs)
	if err != nil {
		return err
	}
	shardTopologyMap := make(map[uint32]*metapb.ShardTopology, len(c.metaData.shardIDs))
	for i, topology := range topologies {
		shardTopologyMap[c.metaData.shardIDs[i]] = topology
	}
	c.metaData.shardTopologyMap = shardTopologyMap
	return nil
}

func (c *Cluster) loadSchemaLocked(ctx context.Context) error {
	schemas, err := c.storage.ListSchemas(ctx, c.clusterID)
	if err != nil {
		return errors.Wrap(err, "cluster loadSchemaLocked")
	}
	schemaMap := make(map[string]*metapb.Schema, len(schemas))
	for _, schema := range schemas {
		schemaMap[schema.Name] = schema
	}
	c.metaData.schemaMap = schemaMap
	return nil
}

func (c *Cluster) loadTableLocked(ctx context.Context) error {
	for _, schema := range c.metaData.schemaMap {
		tables, err := c.storage.ListTables(ctx, c.clusterID, schema.Id)
		if err != nil {
			return errors.Wrap(err, "cluster loadTableLocked")
		}
		for _, table := range tables {
			if t, ok := c.metaData.tableMap[schema.GetName()]; ok {
				t[table.GetId()] = table
			} else {
				c.metaData.tableMap[schema.GetName()] = map[uint64]*metapb.Table{table.GetId(): table}
			}
		}
	}
	return nil
}

func (c *Cluster) getSchema(schemaName string) (*Schema, bool) {
	schema, ok := c.schemas[schemaName]
	return schema, ok
}

func (c *Cluster) getTable(ctx context.Context, schemaName, tableName string) (*Table, bool, error) {
	schema, ok := c.schemas[schemaName]
	if !ok {
		return nil, false, ErrSchemaNotFound.WithCausef("schema_name", schemaName)
	}
	table, exists := schema.getTable(tableName)
	if exists {
		return table, true, nil
	}
	// find in storage
	tablePb, exists, err := c.storage.GetTable(ctx, c.clusterID, schema.GetID(), tableName)
	if err != nil {
		return nil, false, errors.Wrap(err, "getTable")
	}
	if exists {
		c.Lock()
		defer c.Unlock()
		table := c.updateTableCacheLocked(schema, tablePb)
		return table, true, nil
	}

	return nil, false, nil
}

type MetaData struct {
	cluster          *metapb.Cluster
	clusterTopology  *metapb.ClusterTopology
	shardIDs         []uint32
	shardMap         map[uint32][]*metapb.Shard
	shardTopologyMap map[uint32]*metapb.ShardTopology
	schemaMap        map[string]*metapb.Schema
	tableMap         map[string]map[uint64]*metapb.Table // schemas-> ( table_id -> table )
	nodeMap          map[string]*metapb.Node
}
