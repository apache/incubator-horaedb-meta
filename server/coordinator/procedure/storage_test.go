// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

const (
	TestClusterID       = 1
	TestRootPath        = "/rootPath"
	DefaultTimeout      = time.Second * 10
	DefaultScanBatchSie = 100
)

func newTestStorage(t *testing.T) *EtcdStorageImpl {
	_, client, _ := etcdutil.PrepareEtcdServerAndClient(t)
	storage := NewEtcdStorageImpl(client, uint32(TestClusterID), TestRootPath)
	return storage
}

func testWrite(t *testing.T, storage *EtcdStorageImpl) {
	re := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	testMeta1 := &Meta{
		ID:      uint64(1),
		Typ:     TransferLeader,
		State:   StateInit,
		RawData: []byte("test"),
	}

	// Test create new procedure
	err := storage.CreateOrUpdate(ctx, testMeta1)
	re.NoError(err)

	testMeta2 := &Meta{
		ID:      uint64(2),
		Typ:     TransferLeader,
		State:   StateInit,
		RawData: []byte("test"),
	}
	err = storage.CreateOrUpdate(ctx, testMeta2)
	re.NoError(err)

	// Test update procedure
	testMeta2.RawData = []byte("test update")
	err = storage.CreateOrUpdate(ctx, testMeta2)
	re.NoError(err)
}

func testScan(t *testing.T, storage *EtcdStorageImpl) {
	re := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	metas, err := storage.Scan(ctx, DefaultScanBatchSie)
	re.NoError(err)
	re.Equal(2, len(metas))
}

func testDelete(t *testing.T, storage *EtcdStorageImpl) {
	re := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	testMeta1 := &Meta{
		ID:      uint64(1),
		Typ:     TransferLeader,
		State:   StateInit,
		RawData: []byte("test"),
	}
	err := storage.Delete(ctx, testMeta1)
	re.NoError(err)

	metas, err := storage.Scan(ctx, DefaultScanBatchSie)
	re.NoError(err)
	re.Equal(1, len(metas))
}

func TestStorage(t *testing.T) {
	storage := newTestStorage(t)
	testWrite(t, storage)
	testScan(t, storage)
	testDelete(t, storage)
}
