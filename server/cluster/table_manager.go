// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"context"
	"sync"
	"time"

	"github.com/CeresDB/ceresmeta/server/id"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"go.uber.org/zap"

	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
)

// TableManager manages table metadata by schema.
type TableManager interface {
	Load(context.Context) error
	GetTable(schemaName string, tableName string) (storage.Table, bool, error)
	GetTablesByIDs([]storage.TableID) []storage.Table
	CreateTable(ctx context.Context, schemaName string, tableName string) (storage.Table, error)
	DropTable(ctx context.Context, schemaName string, tableName string) error
	GetSchemaByName(string) (storage.Schema, bool)
	GetSchemas() []storage.Schema
	GetOrCreateSchema(context.Context, string) (storage.Schema, bool, error)
}

// nolint
type Tables struct {
	tables     map[string]storage.Table
	tablesByID map[storage.TableID]storage.Table
}

// nolint
type TableManagerImpl struct {
	storage       storage.Storage
	clusterID     storage.ClusterID
	schemaIDAlloc id.Allocator
	tableIDAlloc  id.Allocator

	// RWMutex is used to protect following fields.
	lock         sync.RWMutex
	schemas      map[string]storage.Schema           // schemaName -> schema
	schemasByID  map[storage.SchemaID]storage.Schema // schemaID -> schema
	schemaTables map[storage.SchemaID]*Tables        // schemaName -> Tables
}

func NewTableManagerImpl(storage storage.Storage, clusterID storage.ClusterID, schemaIDAlloc id.Allocator, tableIDAlloc id.Allocator) TableManager {
	return &TableManagerImpl{
		storage:       storage,
		clusterID:     clusterID,
		schemaIDAlloc: schemaIDAlloc,
		tableIDAlloc:  tableIDAlloc,
	}
}

func (m *TableManagerImpl) Load(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if err := m.loadSchema(ctx); err != nil {
		return errors.WithMessage(err, "load schemas")
	}

	if err := m.loadTable(ctx); err != nil {
		return errors.WithMessage(err, "load Tables")
	}

	return nil
}

func (m *TableManagerImpl) GetTable(schemaName, tableName string) (storage.Table, bool, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.getTable(schemaName, tableName)
}

func (m *TableManagerImpl) GetTablesByIDs(tableIDs []storage.TableID) []storage.Table {
	m.lock.RLock()
	defer m.lock.RUnlock()

	result := make([]storage.Table, 0, len(tableIDs))
	for _, tables := range m.schemaTables {
		for _, tableID := range tableIDs {
			table, ok := tables.tablesByID[tableID]
			if !ok {
				log.Warn("table not exists", zap.Uint64("tableID", uint64(tableID)))
				continue
			}
			result = append(result, table)
		}
	}

	return result
}

func (m *TableManagerImpl) CreateTable(ctx context.Context, schemaName string, tableName string) (storage.Table, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	_, exists, err := m.getTable(schemaName, tableName)
	if err != nil {
		return storage.Table{}, errors.WithMessage(err, "create table")
	}

	if exists {
		return storage.Table{}, ErrTableAlreadyExists
	}

	// Create table in storage.
	schema, ok := m.schemas[schemaName]
	if !ok {
		return storage.Table{}, ErrSchemaNotFound.WithCausef("schema name:%d", schemaName)
	}

	id, err := m.tableIDAlloc.Alloc(ctx)
	if err != nil {
		return storage.Table{}, errors.WithMessage(err, "alloc table id")
	}

	table := storage.Table{
		ID:        storage.TableID(id),
		Name:      tableName,
		SchemaID:  schema.ID,
		CreatedAt: uint64(time.Now().UnixMilli()),
	}
	err = m.storage.CreateTable(ctx, storage.CreateTableRequest{
		ClusterID: m.clusterID,
		SchemaID:  schema.ID,
		Table:     table,
	})

	if err != nil {
		return storage.Table{}, errors.WithMessage(err, "create table in storage")
	}

	// Update table in memory.
	_, ok = m.schemaTables[schema.ID]
	if !ok {
		m.schemaTables[schema.ID] = &Tables{
			tables:     make(map[string]storage.Table),
			tablesByID: make(map[storage.TableID]storage.Table),
		}
	}
	tables := m.schemaTables[schema.ID]
	tables.tables[tableName] = table
	tables.tablesByID[table.ID] = table

	return table, nil
}

func (m *TableManagerImpl) DropTable(ctx context.Context, schemaName string, tableName string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	schema, ok := m.schemas[schemaName]
	if !ok {
		return nil
	}

	table, ok := m.schemaTables[schema.ID].tables[tableName]
	if !ok {
		return nil
	}

	// Drop table in storage.
	err := m.storage.DeleteTable(ctx, storage.DeleteTableRequest{
		ClusterID: m.clusterID,
		SchemaID:  schema.ID,
		TableName: tableName,
	})
	if err != nil {
		return errors.WithMessagef(err, "storage delete table, clusterID:%d, schema:%s, tableName:%s",
			m.clusterID, schemaName, tableName)
	}

	tables := m.schemaTables[schema.ID]
	delete(tables.tables, tableName)
	delete(tables.tablesByID, table.ID)
	return nil
}

func (m *TableManagerImpl) GetSchemaByName(schemaName string) (storage.Schema, bool) {
	schema, ok := m.schemas[schemaName]
	return schema, ok
}

func (m *TableManagerImpl) GetSchemas() []storage.Schema {
	m.lock.RLock()
	defer m.lock.RUnlock()

	schemas := make([]storage.Schema, len(m.schemas))

	for _, schema := range m.schemas {
		schemas = append(schemas, schema)
	}

	return schemas
}

func (m *TableManagerImpl) GetOrCreateSchema(ctx context.Context, schemaName string) (storage.Schema, bool, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	schema, ok := m.schemas[schemaName]
	if ok {
		return schema, true, nil
	}

	id, err := m.schemaIDAlloc.Alloc(ctx)
	if err != nil {
		return storage.Schema{}, false, errors.WithMessage(err, "alloc schema id")
	}

	schema = storage.Schema{
		ID:        storage.SchemaID(id),
		ClusterID: m.clusterID,
		Name:      schemaName,
		CreatedAt: uint64(time.Now().UnixMilli()),
	}

	// Create schema in storage.
	if err = m.storage.CreateSchema(ctx, storage.CreateSchemaRequest{
		ClusterID: m.clusterID,
		Schema:    schema,
	}); err != nil {
		return storage.Schema{}, false, errors.WithMessage(err, "create schema in storage")
	}
	// Update schema in memory.
	m.schemas[schemaName] = schema
	return schema, false, nil
}

func (m *TableManagerImpl) loadSchema(ctx context.Context) error {
	schemasResult, err := m.storage.ListSchemas(ctx, storage.ListSchemasRequest{ClusterID: m.clusterID})
	if err != nil {
		return errors.WithMessage(err, "list schemas")
	}

	m.schemas = make(map[string]storage.Schema, len(schemasResult.Schemas))
	for _, schema := range schemasResult.Schemas {
		m.schemas[schema.Name] = schema
	}

	return nil
}

func (m *TableManagerImpl) loadTable(ctx context.Context) error {
	m.schemaTables = make(map[storage.SchemaID]*Tables, len(m.schemas))
	for _, schema := range m.schemas {
		tablesResult, err := m.storage.ListTables(ctx, storage.ListTableRequest{
			ClusterID: m.clusterID,
			SchemaID:  schema.ID,
		})
		if err != nil {
			return errors.WithMessage(err, "list Tables")
		}
		for _, table := range tablesResult.Tables {
			tables, ok := m.schemaTables[table.SchemaID]
			if !ok {
				tables.tables = make(map[string]storage.Table, 0)
				tables.tablesByID = make(map[storage.TableID]storage.Table, 0)
			}

			tables.tables[table.Name] = table
			tables.tablesByID[table.ID] = table
		}
	}
	return nil
}

func (m *TableManagerImpl) getTable(schemaName, tableName string) (storage.Table, bool, error) {
	schema, ok := m.schemas[schemaName]
	if !ok {
		return storage.Table{}, false, ErrSchemaNotFound.WithCausef("schema name", schemaName)
	}
	tables, ok := m.schemaTables[schema.ID]
	if !ok {
		return storage.Table{}, false, nil
	}

	table, ok := tables.tables[tableName]
	return table, ok, nil
}
