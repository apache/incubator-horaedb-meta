// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.
// This file fork from: https://github.com/buraksezer/consistent/blob/4516339c49db00f725fa89d0e3e7e970e4039af0/consistent.go
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

// Package hash provides a consistent hashing function with bounded loads.
// For more information about the underlying algorithm, please take a look at
// https://research.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html
//
// We optimized and simplify this hash algorithm [implementation](https://github.com/buraksezer/consistent/issues/13)
package hash

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
)

var (
	// ErrInsufficientMemberCount represents an error which means there are not enough members to complete the task.
	ErrInsufficientMemberCount = errors.New("insufficient member count")

	// ErrMemberNotFound represents an error which means requested member could not be found in consistent hash ring.
	ErrMemberNotFound = errors.New("member could not be found in ring")

	// ErrHasherNotProvided will be thrown if the hasher is not provided.
	ErrHasherNotProvided = errors.New("hasher is required")

	// ErrMemberNotFound will be thrown if the replication factor is zero or negative.
	ErrPositiveReplicationFactor = errors.New("positive replication factor is required")
)

type Hasher interface {
	Sum64([]byte) uint64
}

// Member interface represents a member in consistent hash ring.
type Member interface {
	String() string
}

// Config represents a structure to control consistent package.
type Config struct {
	// Hasher is responsible for generating unsigned, 64 bit hash of provided byte slice.
	Hasher Hasher

	// Keys are distributed among partitions. Prime numbers are good to
	// distribute keys uniformly. Select a big PartitionCount if you have
	// too many keys.
	ReplicationFactor int
}

// ConsistentUniformHash generates a uniform distribution of partitions over the members, and this distribution will keep as
// consistent as possible while the members has some tiny changes.
type ConsistentUniformHash struct {
	config         Config
	partitionCount uint64
	sortedSet      []uint64
	members        map[string]*Member
	loads          map[string]float64
	partitions     map[int]*Member
	ring           map[uint64]*Member
}

func (c *Config) Sanitize() error {
	if c.Hasher == nil {
		return ErrHasherNotProvided
	}

	if c.ReplicationFactor <= 0 {
		return ErrPositiveReplicationFactor
	}

	return nil
}

// NewUniformConsistentHash creates and returns a new Consistent object.
func NewUniformConsistentHash(partitionNum int, members []Member, config Config) (*ConsistentUniformHash, error) {
	if err := config.Sanitize(); err != nil {
		return nil, err
	}

	numReplicatedNodes := len(members) * config.ReplicationFactor
	c := &ConsistentUniformHash{
		config:         config,
		partitionCount: uint64(partitionNum),
		sortedSet:      make([]uint64, 0, numReplicatedNodes),
		members:        make(map[string]*Member, len(members)),
		loads:          make(map[string]float64, len(members)),
		partitions:     make(map[int]*Member, partitionNum),
		ring:           make(map[uint64]*Member, numReplicatedNodes),
	}

	for _, member := range members {
		c.add(member)
	}
	c.distributePartitions()
	return c, nil
}

func (c *ConsistentUniformHash) MinLoad() float64 {
	return math.Floor(c.AvgLoad())
}

func (c *ConsistentUniformHash) AvgLoad() float64 {
	return float64(c.partitionCount) / float64(len(c.members))
}

func (c *ConsistentUniformHash) MaxLoad() float64 {
	return math.Ceil(c.AvgLoad())
}

func (c *ConsistentUniformHash) distributePartition(partID, idx int) {
	avgLoad := c.AvgLoad()
	ok := c.distributeWithLoad(partID, idx, avgLoad)
	if ok {
		return
	}

	maxLoad := c.MaxLoad()
	ok = c.distributeWithLoad(partID, idx, maxLoad)
	if !ok {
		panic("not enough room to distribute partitions")
	}
}

func (c *ConsistentUniformHash) distributeWithLoad(partID, idx int, allowedLoad float64) bool {
	var count int
	for {
		count++
		if count > len(c.sortedSet) {
			return false
		}
		i := c.sortedSet[idx]
		member := *c.ring[i]
		load := c.loads[member.String()]
		if load+1 <= allowedLoad {
			c.partitions[partID] = &member
			c.loads[member.String()]++
			return true
		}
		idx++
		if idx >= len(c.sortedSet) {
			idx = 0
		}
	}
}

func (c *ConsistentUniformHash) distributePartitions() {
	bs := make([]byte, 8)
	for partID := uint64(0); partID < c.partitionCount; partID++ {
		binary.LittleEndian.PutUint64(bs, partID)
		key := c.config.Hasher.Sum64(bs)
		idx := sort.Search(len(c.sortedSet), func(i int) bool {
			return c.sortedSet[i] >= key
		})
		if idx >= len(c.sortedSet) {
			idx = 0
		}
		c.distributePartition(int(partID), idx)
	}
}

func (c *ConsistentUniformHash) add(member Member) {
	for i := 0; i < c.config.ReplicationFactor; i++ {
		key := []byte(fmt.Sprintf("%s%d", member.String(), i))
		h := c.config.Hasher.Sum64(key)
		c.ring[h] = &member
		c.sortedSet = append(c.sortedSet, h)
	}

	sort.Slice(c.sortedSet, func(i int, j int) bool {
		return c.sortedSet[i] < c.sortedSet[j]
	})

	// Storing member at this map is useful to find backup members of a partition.
	c.members[member.String()] = &member
}

// LoadDistribution exposes load distribution of members.
func (c *ConsistentUniformHash) LoadDistribution() map[string]float64 {
	// Create a thread-safe copy
	res := make(map[string]float64)
	for member, load := range c.loads {
		res[member] = load
	}
	return res
}

// GetPartitionOwner returns the owner of the given partition.
func (c *ConsistentUniformHash) GetPartitionOwner(partID int) Member {
	member, ok := c.partitions[partID]
	if !ok {
		return nil
	}
	// Create a thread-safe copy of member and return it.
	return *member
}
