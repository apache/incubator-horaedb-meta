// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package id

//Allocator defines the id allocator on the ceresdb cluster meta info.
type Allocator interface {
	Alloc() (uint64, error)
	Rebase() error
}
