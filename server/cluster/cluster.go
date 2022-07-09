package cluster

import (
	"context"
	"sync"

	"github.com/CeresDB/ceresdbproto/pkg/metapb"
	"github.com/CeresDB/ceresmeta/server/storage"
)

type Cluster struct {
	sync.RWMutex

	clusterID uint32
	//id

	storage storage.Storage

	metaData MetaData

	shards map[uint32]Shard
	schema map[string]Schema
}

func NewCluster(clusterID uint32, storage storage.Storage) *Cluster {

	return &Cluster{clusterID: clusterID, storage: storage, metaData: MetaData{}}
}

func (c *Cluster) Name() string {
	return c.metaData.cluster.Name
}

func (c *Cluster) Load(ctx context.Context) error {
	c.Lock()
	defer c.Unlock()

	if err := c.loadCluster(ctx); err != nil {
		ErrLoad.WithCause(err)
	}

	if err := c.loadClusterTopology(ctx); err != nil {
		ErrLoad.WithCause(err)
	}

	if err := c.loadShardTopology(ctx); err != nil {
		ErrLoad.WithCause(err)
	}

	if err := c.loadSchema(ctx); err != nil {
		ErrLoad.WithCause(err)
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

type MetaData struct {
	cluster          *metapb.Cluster
	clusterTopology  *metapb.ClusterTopology
	shardIDs         []uint32
	shardMap         map[uint32][]*metapb.Shard
	shardTopologyMap map[uint32]*metapb.ShardTopology
	schemaMap        map[string]*metapb.Schema
	tableMap         map[string]map[string]*metapb.Table // schema-> ( table_name -> table )
}
