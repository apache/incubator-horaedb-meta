// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

import (
	"fmt"
	"path"
)

const (
	cluster = "v1/cluster"
	schema  = "schema"
)

// makeSchemaKey returns the schema meta info key path with the given region ID.
// example:
// cluster 1: v1/cluster/1/schema/1 -> ceresmeta.Schema
//            v1/cluster/1/schema/2 -> ceresmeta.Schema
//            v1/cluster/1/schema/3 -> ceresmeta.Schema
func makeSchemaKey(clusterId uint32, schemaId uint32) string {
	return path.Join(cluster, fmt.Sprintf("%020d", clusterId), schema, fmt.Sprintf("%020d", schemaId))
}