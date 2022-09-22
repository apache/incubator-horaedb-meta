package id

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAlloc(t *testing.T) {
	re := require.New(t)
	cxt := context.Background()
	// IDs: [1,2,3,5,6]
	alloc := NewReusableAllocatorImpl([]uint64{1, 2, 3, 5, 6}, uint64(1))

	// IDs: [1,2,3,5,6]
	// Alloc: 4
	// IDs: [1,2,3,4,5,6]
	id, err := alloc.Alloc(cxt)
	re.NoError(err)
	re.Equal(uint64(4), id)

	// IDs: [1,2,3,5,6]
	// Alloc: 7
	// IDs: [1,2,3,4,5,6,7]
	id, err = alloc.Alloc(cxt)
	re.NoError(err)
	re.Equal(uint64(7), id)

	// IDs: [1,2,3,5,6]
	// Collect: 1
	// IDs: [2,3,4,5,6,7]
	err = alloc.Collect(cxt, uint64(1))
	re.NoError(err)

	// IDs: [2,3,4,5,6,7]
	// Alloc: 1
	// IDs: [1,2,3,4,5,6,7]
	id, err = alloc.Alloc(cxt)
	re.NoError(err)
	re.Equal(uint64(1), id)
}
