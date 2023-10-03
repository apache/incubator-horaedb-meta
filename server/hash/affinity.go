// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package hash

type Affinity struct {
	NumAllowedOtherPartitions uint
}

type AffinityRule struct {
	PartitionAffinities map[int]Affinity
}
