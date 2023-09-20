// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.
// Copyright (c) 2018 Burak Sezer
// All rights reserved.
//
// This code is licensed under the MIT License.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files(the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and / or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions :
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package hash

import (
	"fmt"
	"hash/fnv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newConfig() Config {
	return Config{
		ReplicationFactor: 127,
		Hasher:            hasher{},
	}
}

type testMember string

func (tm testMember) String() string {
	return string(tm)
}

type hasher struct{}

func (hs hasher) Sum64(data []byte) uint64 {
	h := fnv.New64()
	_, _ = h.Write(data)
	return h.Sum64()
}

func TestUniformLoad(t *testing.T) {
	members := []Member{}
	for i := 0; i < 8; i++ {
		member := testMember(fmt.Sprintf("node-%d", i))
		members = append(members, member)
	}
	cfg := newConfig()
	c, err := NewUniformConsistentHash(23, members, cfg)
	assert.NoError(t, err)

	minLoad := c.MinLoad()
	assert.True(t, minLoad >= 1.0)
	maxLoad := c.MaxLoad()
	loadDistribution := c.LoadDistribution()
	for _, mem := range members {
		load, ok := loadDistribution[mem.String()]
		assert.True(t, ok)

		assert.GreaterOrEqual(t, load, minLoad)
		assert.LessOrEqual(t, load, maxLoad)
	}
}

func checkConsistent(t *testing.T, numPartitions, numMembers, maxDiff int) {
	members := make([]Member, 0, numMembers)
	for i := 0; i < numMembers; i++ {
		member := testMember(fmt.Sprintf("node-%d", i))
		members = append(members, member)
	}
	cfg := newConfig()
	c, err := NewUniformConsistentHash(numPartitions, members, cfg)
	assert.NoError(t, err)

	distribution := make(map[int]string, 23)
	for partID := 0; partID < numPartitions; partID++ {
		distribution[partID] = c.GetPartitionOwner(partID).String()
	}

	members[0] = testMember("new-node-0")
	c, err = NewUniformConsistentHash(numPartitions, members, cfg)
	assert.NoError(t, err)

	numDiffs := 0
	for partID := 0; partID < numPartitions; partID++ {
		newMem := c.GetPartitionOwner(partID).String()
		oldMem := distribution[partID]
		if newMem != oldMem {
			numDiffs += 1
		}
	}

	assert.LessOrEqual(t, numDiffs, maxDiff)
}

func TestConsistency(t *testing.T) {
	checkConsistent(t, 120, 20, 10)
	checkConsistent(t, 100, 20, 8)
	checkConsistent(t, 128, 70, 3)
}
