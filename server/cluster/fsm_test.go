package cluster

import (
	"fmt"
	"testing"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/stretchr/testify/assert"
)

func TestTransferLeader(t *testing.T) {
	fsm := NewFSM(clusterpb.ShardRole_FOLLOWER)
	fmt.Println(fsm.Current())

	err := fsm.Event(EventTransferLeaderStart)
	assert.NoError(t, err)
	fmt.Println(fsm.Current())

	err = fsm.Event(EventTransferLeader)
	assert.NoError(t, err)
	fmt.Println(fsm.Current())
}
