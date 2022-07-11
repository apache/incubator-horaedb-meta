// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.
// Copyright 2017 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// fork from https://github.com/tikv/pd/blob/4fc7ede5c67c1e5b8317a7d1185934b0c48d379e/server/storage/storage_test.go

package storage

import (
	"context"
	"path"
	"strconv"
	"testing"

	"github.com/CeresDB/ceresdbproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func TestStorage(t *testing.T) {
	re := require.New(t)
	cfg := newTestSingleConfig(t)
	etcd, err := embed.StartEtcd(cfg)
	re.NoError(err)
	defer etcd.Close()

	ep := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	re.NoError(err)
	rootPath := path.Join("/pd", strconv.FormatUint(100, 10))
	ops := Options{MaxScanLimit: 10, MinScanLimit: 0}

	s := NewStorageWithEtcdBackend(client, rootPath, ops)

	testCluster(re, s)
	testClusterTopology(re, s)
	testSchemes(re, s)
	testTables(re, s)
	testShardTopologies(re, s)
	testNodes(re, s)
}

func testCluster(re *require.Assertions, s Storage) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()
	meta := &metapb.Cluster{}
	err := s.PutCluster(ctx, 0, meta)
	re.NoError(err)
	value, err := s.GetCluster(ctx, 0)
	re.NoError(err)
	re.Equal(meta, value)
}

func testClusterTopology(re *require.Assertions, s Storage) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()
	clusterMetaData := &metapb.ClusterTopology{}
	err := s.PutClusterTopology(ctx, 1, clusterMetaData)
	re.NoError(err)
	value, err := s.GetClusterTopology(ctx, 1)
	re.NoError(err)
	re.Equal(clusterMetaData, value)
}

func testSchemes(re *require.Assertions, s Storage) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()
	schemas := make([]*metapb.Schema, 0)
	for i := 0; i < 10; i++ {
		schema := &metapb.Schema{Id: uint32(i)}
		schemas = append(schemas, schema)
	}
	err := s.PutSchemas(ctx, 0, schemas)
	re.NoError(err)
	value, err := s.ListSchemas(ctx, 0)
	re.NoError(err)
	re.Equal(schemas, value)
}

func testTables(re *require.Assertions, s Storage) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()
	tables := make([]*metapb.Table, 0)
	for i := 0; i < 10; i++ {
		table := &metapb.Table{Id: uint64(i)}
		tables = append(tables, table)
	}
	err := s.PutTables(ctx, 0, 0, tables)
	re.NoError(err)
	tableID := make([]uint64, 0)
	for i := 0; i < 10; i++ {
		tableID = append(tableID, uint64(i))
	}
	value, err := s.ListTables(ctx, 0, 0, tableID)
	re.NoError(err)
	re.Equal(tables, value)
	err = s.DeleteTables(ctx, 0, 0, tableID)
	re.NoError(err)
}

func testShardTopologies(re *require.Assertions, s Storage) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()
	shardTableInfo := make([]*metapb.ShardTopology, 0)
	shardID := make([]uint32, 0)
	for i := 0; i < 10; i++ {
		shardTableData := &metapb.ShardTopology{}
		shardTableInfo = append(shardTableInfo, shardTableData)
		shardID = append(shardID, uint32(i))
	}
	err := s.PutShardTopologies(ctx, 0, shardID, shardTableInfo)
	re.NoError(err)
	value, err := s.ListShardTopologies(ctx, 0, shardID)
	re.NoError(err)
	re.Equal(shardTableInfo, value)
}

func testNodes(re *require.Assertions, s Storage) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()
	nodes := make([]*metapb.Node, 0)
	for i := 0; i < 10; i++ {
		node := &metapb.Node{Id: uint32(i)}
		nodes = append(nodes, node)
	}
	err := s.PutNodes(ctx, 0, nodes)
	re.NoError(err)
	value, err := s.ListNodes(ctx, 0)
	re.NoError(err)
	re.Equal(nodes, value)
}
