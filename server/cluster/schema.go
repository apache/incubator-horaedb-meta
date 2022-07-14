package cluster

import "github.com/CeresDB/ceresdbproto/pkg/metapb"

type Schema struct {
	meta *metapb.Schema

	tableMap map[string]*Table
}

func (s *Schema) GetID() uint32 {
	return s.meta.GetId()
}

func (s *Schema) getTable(tableName string) (*Table, bool) {
	table, ok := s.tableMap[tableName]
	return table, ok
}
