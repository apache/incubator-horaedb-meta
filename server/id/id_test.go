// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package id

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/tempurl"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

const defaultRequestTimeout = time.Second * 10

func TestAlloc(t *testing.T) {
	re := require.New(t)
	cfg := newTestSingleConfig(t)
	etcd, err := embed.StartEtcd(cfg)
	re.NoError(err)
	defer etcd.Close()

	ep := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	re.NoError(err)
	rootPath := path.Join("/ceresmeta", strconv.FormatUint(100, 10))

	allo := NewAllocatorImpl(client, rootPath)
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()
	for i := 0; i < 2010; i++ {
		value, err := allo.Alloc(ctx)
		re.NoError(err)
		re.Equal(uint64(i+1), value)
	}
	err = allo.Rebase(ctx)
	re.NoError(err)
	for i := 0; i < 2010; i++ {
		value, err := allo.Alloc(ctx)
		re.NoError(err)
		re.Equal(uint64(i+3001), value)
	}
}

func newTestSingleConfig(t *testing.T) *embed.Config {
	cfg := embed.NewConfig()
	cfg.Name = "test_etcd"
	cfg.Dir = t.TempDir()
	cfg.WalDir = ""
	cfg.Logger = "zap"
	cfg.LogOutputs = []string{"stdout"}

	pu, _ := url.Parse(tempurl.Alloc())
	cfg.LPUrls = []url.URL{*pu}
	cfg.APUrls = cfg.LPUrls
	cu, _ := url.Parse(tempurl.Alloc())
	cfg.LCUrls = []url.URL{*cu}
	cfg.ACUrls = cfg.LCUrls

	cfg.StrictReconfigCheck = false
	cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, &cfg.LPUrls[0])
	cfg.ClusterState = embed.ClusterStateFlagNew
	return cfg
}
