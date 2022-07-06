package storage

import (
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Storage is the interface for the backend storage of the ceresmeta.
type Storage interface {
	KV
	MetaStorage
}

// NewStorageWithEtcdBackend creates a new storage with etcd backend.
func NewStorageWithEtcdBackend(client *clientv3.Client, rootPath string, requestTimeout time.Duration) Storage {
	return newEtcdStorage(client, rootPath, requestTimeout)
}
