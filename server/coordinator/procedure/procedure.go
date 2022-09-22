// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"github.com/looplab/fsm"
)

type State string

const (
	StateInit      = "init"
	StateRunning   = "running"
	StateFinished  = "finished"
	StateFailed    = "failed"
	StateCancelled = "cancelled"
)

type ShardOperationType uint

const (
	ShardOperationTypeCreate ShardOperationType = iota
	ShardOperationTypeDelete
	ShardOperationTypeTransferLeader
	ShardOperationTypeMigrate
	ShardOperationTypeSplit
	ShardOperationTypeMerge
)

// Procedure is used to describe how to execute a set of operations from the scheduler, e.g. SwitchLeaderProcedure, MergeShardProcedure.
type Procedure interface {
	// ID of the procedure.
	ID() uint64

	// Type of the procedure.
	Type() ShardOperationType

	// Start the procedure.
	Start(ctx context.Context) error

	// Cancel the procedure.
	Cancel(ctx context.Context) error

	// State of the procedure. Retrieve the state of this procedure.
	State() State
}

// nolint
type Manager struct {
	storage    *Storage
	procedures []Procedure
}

func NewProcedure(operationType ShardOperationType) *fsm.FSM {
	switch operationType {
	case ShardOperationTypeCreate:
		return nil
	case ShardOperationTypeDelete:
		return nil
	case ShardOperationTypeTransferLeader:
		//NewTransferLeaderProcedure()
		return nil
	case ShardOperationTypeMigrate:
		return nil
	case ShardOperationTypeSplit:
		return nil
	case ShardOperationTypeMerge:
		return nil
	}

	return nil
}
