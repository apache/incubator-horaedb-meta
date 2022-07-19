package cluster

import "github.com/CeresDB/ceresdbproto/pkg/clusterpb"

type Table struct {
	schema *clusterpb.Schema

	meta *clusterpb.Table
}

func (t *Table) getID() uint64 {
	return t.meta.GetId()
}
