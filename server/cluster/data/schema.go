// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package data

import (
	"sync"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/server/storage"
)

// nolint
type Schema struct {
	storage storage.Storage

	// RWMutex is used to protect following fields.
	lock         sync.RWMutex
	meta         *clusterpb.Schema  // schema meta info in storage
	schemaTables map[string]*Tables // schemaName -> tables
}

// nolint
type Tables struct {
	tables map[string]*clusterpb.Table
}
