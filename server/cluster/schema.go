package cluster

import "github.com/CeresDB/ceresdbproto/pkg/metapb"

type Schema struct {
	meta *metapb.Schema

	tableMap map[string]*Table
}
