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
	schemaRWMutex sync.RWMutex

	clusterID uint32
	//id

	storage storage.Storage

	metaData MetaData

	shards map[uint32]*Shard
	schema map[string]*Schema
	nodes  map[string][]*Shard
}

func NewCluster(clusterID uint32, storage storage.Storage) *Cluster {

	return &Cluster{schemaRWMutex: sync.RWMutex{}, clusterID: clusterID, storage: storage, metaData: MetaData{}}
}

func (c *Cluster) Name() string {
	return c.metaData.cluster.Name
}

func (c *Cluster) Load(ctx context.Context) error {
	c.Lock()
	defer c.Unlock()

	if err := c.loadCluster(ctx); err != nil {
		errors.Wrapf(err, "Load")
	}

	if err := c.loadClusterTopology(ctx); err != nil {
		errors.Wrapf(err, "Load")
	}

	if err := c.loadShardTopology(ctx); err != nil {
		errors.Wrapf(err, "Load")
	}

	if err := c.loadSchema(ctx); err != nil {
		errors.Wrapf(err, "Load")
	}

	return nil
}

func (c *Cluster) GetTables(ctx context.Context, shardIDs []uint32, node string) (map[uint32]*ShardTablesWithRole, error) {
	shardTables := make(map[uint32]*ShardTablesWithRole, len(shardIDs))
	for _, shardID := range shardIDs {
		shardTable, ok := c.shards[shardID]
		if !ok {
			return nil, ErrShardNotFound.WithCausef("shard_id", shardID)
		}

		shardRole := FOLLOWER
		for i, n := range shardTable.node {
			if node == n.GetNodeStats().GetNode() {
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
	c.schemaRWMutex.Lock()
	defer c.schemaRWMutex.Unlock()
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
	schema := &Schema{meta: schemaPb}
	c.schema[schemaPb.GetName()] = schema
	return schema, nil
}

func (c *Cluster) CreateTable(ctx context.Context, schemaName string, shardID uint32, tableName string, tableID uint64) (*Table, error) {
	c.schemaRWMutex.Lock()
	defer c.schemaRWMutex.Unlock()
	// check if exists
	schema, exists := c.getSchema(ctx, schemaName)
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
	table := &Table{meta: tablePb, schema: schema.meta}
	schema.tableMap[tableName] = table
	c.shards[shardID].tables = append(c.shards[shardID].tables, table)
	return table, nil
}

func (c *Cluster) loadCluster(ctx context.Context) error {
	cluster, err := c.storage.GetCluster(ctx, c.clusterID)
	if err != nil {
		return err
	}
	c.metaData.cluster = cluster
	return nil
}

func (c *Cluster) loadClusterTopology(ctx context.Context) error {
	clusterTopology, err := c.storage.GetClusterTopology(ctx, c.clusterID)
	if err != nil {
		return err
	}
	c.metaData.clusterTopology = clusterTopology

	shardIDs := make([]uint32, len(c.metaData.clusterTopology.ShardView), len(c.metaData.clusterTopology.ShardView))
	shardMap := make(map[uint32][]*metapb.Shard)

	for _, shard := range c.metaData.clusterTopology.ShardView {
		shardMap[shard.Id] = append(shardMap[shard.Id], shard)
		shardIDs = append(shardIDs, shard.Id)
	}
	c.metaData.shardMap = shardMap
	c.metaData.shardIDs = shardIDs
	return nil
}

func (c *Cluster) loadShardTopology(ctx context.Context) error {
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

func (c *Cluster) loadSchema(ctx context.Context) error {
	schemas, err := c.storage.ListSchemas(ctx, c.clusterID)
	if err != nil {
		return err
	}
	schemaMap := make(map[string]*metapb.Schema, len(schemas))
	for _, schema := range schemas {
		schemaMap[schema.Name] = schema
	}
	c.metaData.schemaMap = schemaMap
	return nil
}

func (c *Cluster) getSchema(ctx context.Context, schemaName string) (*Schema, bool) {
	schema, ok := c.schema[schemaName]
	return schema, ok
}

func (c *Cluster) getTable(ctx context.Context, schemaName, tableName string) (*Table, bool, error) {
	schema, ok := c.schema[schemaName]
	if !ok {
		return nil, false, ErrSchemaNotFound.WithCausef("schema_name", schemaName)
	}
	table, exists := schema.getTable(tableName)
	if exists {
		return table, true, nil
	}
	// find in storage
	tablePb, exists, err1 := c.storage.GetTable(ctx, c.clusterID, schema.GetID(), tableName)
	if err1 != nil {
		return nil, false, errors.Wrap(err1, "getTable")
	}
	table1, err1 := c.addTable(ctx, schemaName, tablePb)
	if err1 != nil {
		return nil, false, errors.Wrap(err1, "getTable")
	}
	return table1, true, nil
}

// add table to cache
func (c *Cluster) addTable(ctx context.Context, schemaName string, tablePb *metapb.Table) (*Table, error) {
	// todo
	return nil, nil
}

type MetaData struct {
	cluster          *metapb.Cluster
	clusterTopology  *metapb.ClusterTopology
	shardIDs         []uint32
	shardMap         map[uint32][]*metapb.Shard
	shardTopologyMap map[uint32]*metapb.ShardTopology
	schemaMap        map[string]*metapb.Schema
	tableMap         map[string]map[string]*metapb.Table // schema-> ( table_name -> table )
}
