// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
)

type State string

const (
	StateInit      = "init"
	StateRunning   = "running"
	StateFinished  = "finished"
	StateFailed    = "failed"
	StateCancelled = "cancelled"
)

type Typ uint

const (
	// Cluster Operation
	Create Typ = iota
	Delete
	TransferLeader
	Migrate
	Split
	Merge
	Scatter

	// DDL
	CreateTable
	DropTable
	CreatePartitionTable
	DropPartitionTable
)

// Procedure is used to describe how to execute a set of operations from the scheduler, e.g. SwitchLeaderProcedure, MergeShardProcedure.
type Procedure interface {
	// ID of the procedure.
	ID() uint64

	// Typ of the procedure.
	Typ() Typ

	// Start the procedure.
	Start(ctx context.Context) error

	// Cancel the procedure.
	Cancel(ctx context.Context) error

	// State of the procedure. Retrieve the state of this procedure.
	State() State

	// GetShardID return the shardID associated with this procedure.
	// TODO: Some procedure may be associated with multi shards, it should be supported.
	GetShardID() uint64

	GetShardVersion() uint64

	// GetClusterVersion return the cluster version when the procedure is created.
	// When performing cluster operation, it is necessary to ensure cluster version consistency.
	GetClusterVersion() uint64
}

// Info is used to provide immutable description procedure information.
type Info struct {
	ID    uint64
	Typ   Typ
	State State
}

// IsDDL used to check procedure operation type.
// Procedure only has two operation type: 1. DDL  2. Cluster Operation
// We need ensure shardVersion consist when it is DDL and cluster version is consist when it is cluster operation.
func IsDDL(p Procedure) bool {
	if p.Typ() == CreateTable || p.Typ() == DropTable || p.Typ() == CreatePartitionTable || p.Typ() == DropPartitionTable {
		return true
	}
	return false
}
