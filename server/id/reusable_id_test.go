package id

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	maxShardNum = 64
)

func TestAlloc(t *testing.T) {
	re := require.New(t)
	cxt := context.Background()
	alloc := NewReusableAllocatorImpl(maxShardNum)

	for i := 0; i < maxShardNum; i++ {
		id, err := alloc.Alloc(cxt)
		re.NoError(err)
		re.Equal(uint64(i), id)
	}

	_, err := alloc.Alloc(cxt)
	re.Error(err)

	err = alloc.Collect(100)
	re.Error(err)

	err = alloc.Collect(0)
	re.NoError(err)

	id, err := alloc.Alloc(cxt)
	re.NoError(err)
	re.Equal(uint64(0), id)
}
