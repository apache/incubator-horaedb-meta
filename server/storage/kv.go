// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package storage

// Base is an abstract interface for kv storage
type Base interface {
	Load(key string) (string, error)
	LoadRange(key, endKey string, limit int) (keys []string, values []string, err error)
	Save(key, value string) error
	Remove(key string) error
}
