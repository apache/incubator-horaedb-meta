package cluster

import "github.com/CeresDB/ceresdbproto/pkg/metapb"

type Table struct {
	schema *metapb.Schema

	meta *metapb.Table
}
