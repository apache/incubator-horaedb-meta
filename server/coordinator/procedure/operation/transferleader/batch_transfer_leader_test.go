// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package transferleader_test

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/operation/transferleader"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
)

type mockProcedure struct {
	ClusterID        storage.ClusterID
	clusterVersion   uint64
	typ              procedure.Typ
	ShardWithVersion map[storage.ShardID]uint64
}

func (m mockProcedure) ID() uint64 {
	return 0
}

func (m mockProcedure) Typ() procedure.Typ {
	return m.typ
}

func (m mockProcedure) Start(_ context.Context) error {
	return nil
}

func (m mockProcedure) Cancel(_ context.Context) error {
	return nil
}

func (m mockProcedure) State() procedure.State {
	return procedure.StateInit
}

func (m mockProcedure) RelatedVersionInfo() procedure.RelatedVersionInfo {
	return procedure.RelatedVersionInfo{
		ClusterID:        m.ClusterID,
		ShardWithVersion: m.ShardWithVersion,
		ClusterVersion:   m.clusterVersion,
	}
}

func (m mockProcedure) Priority() procedure.Priority {
	return procedure.PriorityLow
}

func TestBatchProcedure(t *testing.T) {
	re := require.New(t)

	var procedures []procedure.Procedure

	// Procedures with same type and version.
	for i := 0; i < 3; i++ {
		shardWithVersion := map[storage.ShardID]uint64{}
		shardWithVersion[storage.ShardID(i)] = 0
		p := mockProcedure{
			ClusterID:        0,
			clusterVersion:   0,
			typ:              0,
			ShardWithVersion: shardWithVersion,
		}
		procedures = append(procedures, p)
	}
	_, err := transferleader.NewBatchTransferLeaderProcedure(0, procedures)
	re.NoError(err)

	// Procedure with different clusterID
	for i := 0; i < 3; i++ {
		shardWithVersion := map[storage.ShardID]uint64{}
		shardWithVersion[storage.ShardID(i)] = 0
		p := mockProcedure{
			ClusterID:        storage.ClusterID(i),
			clusterVersion:   0,
			typ:              procedure.TransferLeader,
			ShardWithVersion: shardWithVersion,
		}
		procedures = append(procedures, p)
	}
	_, err = transferleader.NewBatchTransferLeaderProcedure(0, procedures)
	re.Error(err)

	// Procedures with different type.
	for i := 0; i < 3; i++ {
		shardWithVersion := map[storage.ShardID]uint64{}
		shardWithVersion[storage.ShardID(i)] = 0
		p := mockProcedure{
			ClusterID:        0,
			clusterVersion:   0,
			typ:              procedure.Typ(i),
			ShardWithVersion: shardWithVersion,
		}
		procedures = append(procedures, p)
	}
	_, err = transferleader.NewBatchTransferLeaderProcedure(0, procedures)
	re.Error(err)

	// Procedures with different version.
	for i := 0; i < 3; i++ {
		shardWithVersion := map[storage.ShardID]uint64{}
		shardWithVersion[storage.ShardID(0)] = uint64(i)
		p := mockProcedure{
			ClusterID:        0,
			clusterVersion:   0,
			typ:              procedure.Typ(i),
			ShardWithVersion: shardWithVersion,
		}
		procedures = append(procedures, p)
	}
	_, err = transferleader.NewBatchTransferLeaderProcedure(0, procedures)
	re.Error(err)
}
