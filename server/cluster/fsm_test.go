package cluster

import (
	"testing"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/stretchr/testify/assert"
)

func TestTransferLeader(t *testing.T) {
	fsm := NewFSM(clusterpb.ShardRole_FOLLOWER)

	err := fsm.Event(EventTransferLeaderStart)
	assert.NoError(t, err)

	err = fsm.Event(EventTransferLeader)
	assert.NoError(t, err)
}
