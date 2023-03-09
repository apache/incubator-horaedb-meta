// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	TestRootPath  = "/rootPath"
	TestShardPath = "shards"
	TestShardID   = 1
	TestNodeName  = "testNode"
)

func TestWatch(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()

	_, client, _ := etcdutil.PrepareEtcdServerAndClient(t)
	watch := NewWatch(TestRootPath, client)
	err := watch.Start(ctx)
	re.NoError(err)

	callbackResult := 0
	testCallback := func(shardID storage.ShardID, nodeName string, eventType ShardEventType) error {
		switch eventType {
		case EventDelete:
			callbackResult = 1
			re.Equal(storage.ShardID(TestShardID), shardID)
			re.Equal(TestNodeName, nodeName)
		case EventPut:
			callbackResult = 2
			re.Equal(storage.ShardID(TestShardID), shardID)
			re.Equal(TestNodeName, nodeName)
		}
		return nil
	}
	watch.RegisteringEventCallback(testCallback)

	// Valid that callback function is executed and the params are as expected.
	keyPath := strings.Join([]string{TestRootPath, TestShardPath, strconv.Itoa(TestShardID)}, "/")
	_, err = client.Put(ctx, keyPath, TestNodeName)
	re.NoError(err)
	time.Sleep(time.Millisecond * 10)
	re.Equal(callbackResult, 2)

	_, err = client.Delete(ctx, keyPath, clientv3.WithPrevKV())
	re.NoError(err)
	time.Sleep(time.Millisecond * 10)
	re.Equal(callbackResult, 1)
}
