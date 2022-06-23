// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package member

import (
	"github.com/CeresDB/ceresdbproto/go/ceresdbproto"
	"github.com/CeresDB/ceresmeta/server/etcdutil"
)

// Member manages the
type Member struct {
	ID        uint64
	Name      string
	clusterKV etcdutil.ClusterKV
	leader    *ceresdbproto.Member
}

func NewMember(id uint64, clusterKV etcdutil.ClusterKV) *Member {
	return &Member{
		ID:        id,
		clusterKV: clusterKV,
		leader:    nil,
	}
}

func (m *Member) GetLeader() (GetLeaderResp, error) {
	return GetLeaderResp{}, nil
}

func (m *Member) ResetLeader() error {
	return nil
}

func (m *Member) WaitForLeaderChange() error {
	return nil
}

func (m *Member) CampaignAndKeepLeader() error {
	return nil
}

type GetLeaderResp struct {
	Leader *ceresdbproto.Member
}
