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

type testMember string

func (tm testMember) String() string {
	return string(tm)
}

type testHasher struct{}

func (hs testHasher) Sum64(data []byte) uint64 {
	h := fnv.New64()
	_, _ = h.Write(data)
	return h.Sum64()
}

func buildTestMembers(n int) []Member {
	members := []Member{}
	for i := 0; i < n; i++ {
		member := testMember(fmt.Sprintf("node-%d", i))
		members = append(members, member)
	}

	return members
}

func checkUniform(t *testing.T, numPartitions, numMembers int) {
	members := buildTestMembers(numMembers)
	cfg := Config{
		ReplicationFactor: 127,
		Hasher:            testHasher{},
	}
	c, err := BuildConsistentUniformHash(numPartitions, members, cfg)
	assert.NoError(t, err)

	minLoad := c.MinLoad()
	maxLoad := c.MaxLoad()
	loadDistribution := c.LoadDistribution()
	for _, mem := range members {
		load, ok := loadDistribution[mem.String()]
		if ok {
			assert.GreaterOrEqual(t, load, minLoad)
			assert.LessOrEqual(t, load, maxLoad)
		} else {
			assert.Equal(t, 0.0, minLoad)
		}
	}
}

func TestZeroReplicationFactor(t *testing.T) {
	cfg := Config{
		ReplicationFactor: 0,
		Hasher:            testHasher{},
	}
	_, err := BuildConsistentUniformHash(0, []Member{testMember("")}, cfg)
	assert.Error(t, err)
}

func TestEmptyHasher(t *testing.T) {
	cfg := Config{
		ReplicationFactor: 127,
		Hasher:            nil,
	}
	_, err := BuildConsistentUniformHash(0, []Member{testMember("")}, cfg)
	assert.Error(t, err)
}

func TestEmptyMembers(t *testing.T) {
	cfg := Config{
		ReplicationFactor: 127,
		Hasher:            testHasher{},
	}
	_, err := BuildConsistentUniformHash(0, []Member{}, cfg)
	assert.Error(t, err)
}

func TestNegativeNumPartitions(t *testing.T) {
	cfg := Config{
		ReplicationFactor: 127,
		Hasher:            testHasher{},
	}
	_, err := BuildConsistentUniformHash(-1, []Member{testMember("")}, cfg)
	assert.Error(t, err)
}

func TestUniform(t *testing.T) {
	checkUniform(t, 23, 8)
	checkUniform(t, 128, 72)
	checkUniform(t, 10, 72)
	checkUniform(t, 1, 8)
	checkUniform(t, 0, 8)
	checkUniform(t, 100, 1)
}

func computeDiffBetweenDist(t *testing.T, oldDist, newDist map[int]string) int {
	numDiffs := 0
	assert.Equal(t, len(oldDist), len(newDist))
	for partID, oldMem := range oldDist {
		newMem, ok := newDist[partID]
		assert.True(t, ok)
		if newMem != oldMem {
			numDiffs++
		}
	}

	return numDiffs
}

func checkConsistent(t *testing.T, numPartitions, numMembers, maxDiff int) {
	members := buildTestMembers(numMembers)
	cfg := Config{
		ReplicationFactor: 127,
		Hasher:            testHasher{},
	}
	c, err := BuildConsistentUniformHash(numPartitions, members, cfg)
	assert.NoError(t, err)

	distribution := make(map[int]string, numPartitions)
	for partID := 0; partID < numPartitions; partID++ {
		distribution[partID] = c.GetPartitionOwner(partID).String()
	}

	{
		c, err := BuildConsistentUniformHash(numPartitions, members, cfg)
		assert.NoError(t, err)
		newDistribution := make(map[int]string, numPartitions)
		for partID := 0; partID < numPartitions; partID++ {
			newDistribution[partID] = c.GetPartitionOwner(partID).String()
		}
		numDiffs := computeDiffBetweenDist(t, distribution, newDistribution)
		assert.Equal(t, numDiffs, 0)
	}

	oldMem0 := members[0].String()
	newMem0 := "new-node-0"
	members[0] = testMember(newMem0)
	c, err = BuildConsistentUniformHash(numPartitions, members, cfg)
	assert.NoError(t, err)

	numDiffs := 0
	for partID := 0; partID < numPartitions; partID++ {
		newMem := c.GetPartitionOwner(partID).String()
		oldMem := distribution[partID]
		if newMem0 == newMem && oldMem != oldMem0 {
			numDiffs++
			continue
		}

		if newMem != oldMem {
			numDiffs++
		}
	}

	assert.LessOrEqual(t, numDiffs, maxDiff)
}

func TestConsistency(t *testing.T) {
	checkConsistent(t, 120, 20, 30)
	checkConsistent(t, 100, 20, 25)
	checkConsistent(t, 128, 70, 26)
	checkConsistent(t, 256, 30, 70)
	checkConsistent(t, 17, 5, 7)
}

func checkAffinity(t *testing.T, numPartitions, numMembers int, rule AffinityRule, revisedMaxLoad uint) {
	members := buildTestMembers(numMembers)
	cfg := Config{
		ReplicationFactor: 127,
		Hasher:            testHasher{},
		AffinityRule:      rule,
	}
	c, err := BuildConsistentUniformHash(numPartitions, members, cfg)
	assert.NoError(t, err)

	minLoad := c.MinLoad()
	maxLoad := c.MaxLoad()
	if maxLoad < revisedMaxLoad {
		maxLoad = revisedMaxLoad
	}
	loadDistribution := c.LoadDistribution()
	for _, mem := range members {
		load, ok := loadDistribution[mem.String()]
		if !ok {
			assert.Equal(t, 0.0, minLoad)
		}
		assert.LessOrEqual(t, load, maxLoad)
	}

	for partID, affinity := range rule.PartitionAffinities {
		mem := c.GetPartitionOwner(partID)
		load := loadDistribution[mem.String()]
		allowedMaxLoad := affinity.NumAllowedOtherPartitions + 1
		assert.LessOrEqual(t, load, allowedMaxLoad)
	}
}

func TestAffinity(t *testing.T) {
	rule := AffinityRule{
		PartitionAffinities: map[int]Affinity{},
	}
	checkAffinity(t, 120, 72, rule, 0)
	checkAffinity(t, 0, 72, rule, 0)

	rule = AffinityRule{
		PartitionAffinities: map[int]Affinity{
			0: {0},
			1: {0},
			2: {120},
		},
	}
	checkAffinity(t, 120, 72, rule, 0)
	checkAffinity(t, 3, 72, rule, 0)
	checkAffinity(t, 72, 72, rule, 0)
}
