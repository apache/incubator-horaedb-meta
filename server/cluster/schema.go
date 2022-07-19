package cluster

import "github.com/CeresDB/ceresdbproto/pkg/clusterpb"

type Schema struct {
	meta *clusterpb.Schema

	tableMap map[string]*Table
}

func (s *Schema) GetID() uint32 {
	return s.meta.GetId()
}

func (s *Schema) getTable(tableName string) (*Table, bool) {
	table, ok := s.tableMap[tableName]
	return table, ok
}
