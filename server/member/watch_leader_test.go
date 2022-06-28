// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package member

import (
	"context"
	"testing"
	"time"

	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver"
)

type mockWatchCtx struct {
	stopped bool
	client  *clientv3.Client
	srv     *etcdserver.EtcdServer
}

func (ctx *mockWatchCtx) ShouldStop() bool {
	return ctx.stopped
}

func (ctx *mockWatchCtx) EtcdLeaderID() uint64 {
	return ctx.srv.Lead()
}

func (ctx *mockWatchCtx) NewLease() clientv3.Lease {
	return clientv3.NewLease(ctx.client)
}

func (ctx *mockWatchCtx) NewWatcher() clientv3.Watcher {
	return clientv3.NewWatcher(ctx.client)
}

func TestWatchLeaderSingle(t *testing.T) {
	cfg := etcdutil.NewTestSingleConfig()
	etcd, err := embed.StartEtcd(cfg)
	defer func() {
		etcd.Close()
		etcdutil.CleanConfig(cfg)
	}()
	assert.NoError(t, err)

	endpoint := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint},
	})
	assert.NoError(t, err)

	<-etcd.Server.ReadyNotify()

	watchCtx := &mockWatchCtx{
		stopped: false,
		client:  client,
		srv:     etcd.Server,
	}
	leaderGetter := &etcdutil.LeaderGetterWrapper{Server: etcd.Server}
	rpcTimeout := time.Duration(10) * time.Second
	leaseTTLSec := int64(1)
	mem := NewMember("", uint64(etcd.Server.ID()), "mem0", client, leaderGetter, rpcTimeout)
	leaderWatcher := NewLeaderWatcher(watchCtx, mem, leaseTTLSec)

	ctx, cancelWatch := context.WithCancel(context.Background())
	watchedDone := make(chan struct{}, 1)
	go func() {
		leaderWatcher.Watch(ctx)
		watchedDone <- struct{}{}
	}()

	// Wait for watcher starting
	time.Sleep(time.Duration(200) * time.Millisecond)

	// check the member has been the leader
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := mem.GetLeader(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, resp.Leader.Id, mem.ID)

	// cancel the watch
	cancelWatch()
	<-watchedDone

	// check again whether the leader is still the `mem`
	ctx, cancel = context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err = mem.GetLeader(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, resp.Leader.Id, mem.ID)

	// wait for the expiration of leader lease.
	time.Sleep(time.Duration(leaseTTLSec) * time.Second * 2)

	// the leader should not be the `mem`
	ctx, cancel = context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err = mem.GetLeader(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Leader)
}
