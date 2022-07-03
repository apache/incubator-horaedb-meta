// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.
// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// fork from: https://github.com/tikv/pd/blob/master/server/storage/endpoint

package storage

import (
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// StorageImpl is the base underlying storage endpoint for all other upper
// specific storage backends. It should define some common storage interfaces and operations,
// which provides the default implementations for all kinds of storages.
type StorageImpl struct {
	KV
}

// NewStorageImpl creates a new base storage endpoint with the given KV and encryption key manager.
// It should be embedded inside a storage backend.
func NewStorageImpl(
	kv KV,
) *StorageImpl {
	return &StorageImpl{
		kv,
	}
}

// newEtcdBackend is used to create a new etcd backend.
func newEtcdStorage(client *clientv3.Client, rootPath string, requestTimeout time.Duration) *StorageImpl {
	return NewStorageImpl(
		NewEtcdKV(client, rootPath, requestTimeout))
}
