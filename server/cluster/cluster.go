package cluster

import (
	"context"
	"sync"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
)

type Cluster struct {
	sync.RWMutex
	clusterID uint32

	storage storage.Storage

	metaData *MetaData

	shards  map[uint32]*Shard   // shard_id -> shard
	schemas map[string]*Schema  // schema_name -> schema
	nodes   map[string][]uint32 // node_name -> shard_ids

	coordinator *coordinator
}

func NewCluster(cluster *clusterpb.Cluster, storage storage.Storage) *Cluster {
	return &Cluster{clusterID: cluster.GetId(), storage: storage, metaData: &MetaData{cluster: cluster},
		shards: make(map[uint32]*Shard), schemas: make(map[string]*Schema), nodes: make(map[string][]uint32)}
}

func (c *Cluster) Name() string {
	return c.metaData.cluster.Name
}

func (c *Cluster) Load(ctx context.Context) error {
	c.Lock()
	defer c.Unlock()

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
		var tables map[uint64]*Table

		for _, tableID := range shardTopology.TableIds {
			for schemaName, tableMap := range c.metaData.tableMap {
				table, ok := tableMap[tableID]
				if ok {
					tables[tableID] = &Table{
						schema: c.metaData.schemaMap[schemaName],
						meta:   table,
					}
				}
			}
		}
		// todo: assert shardID
		// todo: check shard not found by shardID
		shardMetaList := c.metaData.shardMap[shardID]
		var nodes []*clusterpb.Node
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

func (c *Cluster) updateSchemaCacheLocked(schemaPb *clusterpb.Schema) *Schema {
	schema := &Schema{meta: schemaPb}
	c.schemas[schemaPb.GetName()] = schema
	return schema
}

func (c *Cluster) updateTableCacheLocked(schema *Schema, tablePb *clusterpb.Table) *Table {
	table := &Table{meta: tablePb, schema: schema.meta}
	schema.tableMap[tablePb.GetName()] = table
	c.shards[tablePb.GetShardId()].tables[table.getID()] = table
	return table
}

func (c *Cluster) GetTables(ctx context.Context, shardIDs []uint32, node string) (map[uint32]*ShardTablesWithRole, error) {
	shardTables := make(map[uint32]*ShardTablesWithRole, len(shardIDs))
	for _, shardID := range shardIDs {
		shardTable, ok := c.shards[shardID]
		if !ok {
			return nil, ErrShardNotFound.WithCausef("shardID", shardID)
		}

		shardRole := FOLLOWER
		for i, n := range shardTable.nodes {
			if node == n.GetName() {
				if shardTable.meta[i].ShardRole == clusterpb.ShardRole_LEADER {
					shardRole = LEADER
				} else {
					shardRole = FOLLOWER
				}
				break
			}
		}
		tables := make([]*Table, len(shardTable.tables))
		for _, table := range shardTable.tables {
			tables = append(tables, table)
		}
		shardTables[shardID] = &ShardTablesWithRole{shardRole: shardRole, tables: tables, version: shardTable.version}
	}

	return shardTables, nil
}

func (c *Cluster) DropTable(ctx context.Context, schemaName, tableName string, tableID uint64) error {
	schema, exists := c.getSchema(schemaName)
	if exists {
		return ErrSchemaNotFound.WithCausef("schemaName:%s", schemaName)
	}
	if err := c.storage.DeleteTables(ctx, c.clusterID, schema.GetID(), []uint64{tableID}); err != nil {
		return errors.Wrapf(err, "cluster DropTable, "+
			"clusterID:%d, schema:%v, tableID:%d", c.clusterID, schema, tableID)
	}
	c.Lock()
	defer c.Unlock()
	schema.dropTableLocked(tableName)
	for _, shard := range c.shards {
		shard.dropTableLocked(tableID)
	}
	return nil
}

func (c *Cluster) CreateSchema(ctx context.Context, schemaName string, schemaID uint32) (*Schema, error) {
	c.Lock()
	defer c.Unlock()
	// check if exists
	{
		schema, exists := c.getSchema(schemaName)
		if exists {
			return schema, nil
		}
	}

	// persist
	schemaPb := &clusterpb.Schema{Id: schemaID, Name: schemaName, ClusterId: c.clusterID}
	err := c.storage.CreateSchema(ctx, c.clusterID, schemaPb)
	if err != nil {
		return nil, errors.Wrap(err, "cluster CreateSchema")
	}

	// cache
	schema := c.updateSchemaCacheLocked(schemaPb)
	return schema, nil
}

func (c *Cluster) CreateTable(ctx context.Context, schemaName string, shardID uint32,
	tableName string, tableID uint64) (*Table, error) {
	c.Lock()
	defer c.Unlock()
	// check if exists
	schema, exists := c.getSchema(schemaName)
	if !exists {
		return nil, ErrSchemaNotFound.WithCausef("schemaName", schemaName)
	}

	// persist
	tablePb := &clusterpb.Table{Id: tableID, Name: tableName, SchemaId: schema.GetID(), ShardId: shardID}
	err1 := c.storage.CreateTable(ctx, c.clusterID, schema.GetID(), tablePb)
	if err1 != nil {
		return nil, errors.Wrap(err1, "cluster CreateTable")
	}

	// update cache
	table := c.updateTableCacheLocked(schema, tablePb)
	return table, nil
}

func (c *Cluster) loadClusterTopologyLocked(ctx context.Context) error {
	clusterTopology, err := c.storage.GetClusterTopology(ctx, c.clusterID)
	if err != nil {
		return errors.Wrap(err, "cluster loadClusterTopologyLocked")
	}
	c.metaData.clusterTopology = clusterTopology

	if c.metaData.clusterTopology == nil {
		return ErrClusterTopologyNotFound.WithCausef("cluster:%v", c)
	}
	shardIDs := make([]uint32, len(c.metaData.clusterTopology.ShardView))
	shardMap := make(map[uint32][]*clusterpb.Shard)

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
		return errors.Wrap(err, "cluster loadShardTopologyLocked")
	}
	shardTopologyMap := make(map[uint32]*clusterpb.ShardTopology, len(c.metaData.shardIDs))
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
	schemaMap := make(map[string]*clusterpb.Schema, len(schemas))
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
				c.metaData.tableMap[schema.GetName()] = map[uint64]*clusterpb.Table{table.GetId(): table}
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
		return nil, false, ErrSchemaNotFound.WithCausef("schemaName", schemaName)
	}
	table, exists := schema.getTable(tableName)
	if exists {
		return table, true, nil
	}
	// find in storage
	tablePb, exists, err := c.storage.GetTable(ctx, c.clusterID, schema.GetID(), tableName)
	if err != nil {
		return nil, false, errors.Wrap(err, "cluster getTable")
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
	cluster          *clusterpb.Cluster
	clusterTopology  *clusterpb.ClusterTopology
	shardIDs         []uint32
	shardMap         map[uint32][]*clusterpb.Shard          // shard_id -> shard
	shardTopologyMap map[uint32]*clusterpb.ShardTopology    // shard_id -> shardTopology
	schemaMap        map[string]*clusterpb.Schema           // schema_name -> schema
	tableMap         map[string]map[uint64]*clusterpb.Table // schema_name-> ( table_id -> table )
	nodeMap          map[string]*clusterpb.Node             // node_name -> node
}
