// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package member

import (
	"context"
	"time"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"go.uber.org/zap"
)

const (
	WatchLeaderFailInterval = time.Duration(200) * time.Millisecond

	waitReasonFailEtcd    = "fail to access etcd"
	waitReasonResetLeader = "leader is reset"
	waitReasonElectLeader = "leader is electing"
	waitReasonNoWait      = ""
)

type WatchContext interface {
	EtcdLeaderID() uint64
	ShouldStop() bool
}

type LeaderWatcher struct {
	watchCtx WatchContext
	self     *Member
}

func NewLeaderWatcher(ctx WatchContext, self *Member) *LeaderWatcher {
	return &LeaderWatcher{
		ctx,
		self,
	}
}

func (l *LeaderWatcher) Watch(_ context.Context) {
	var wait string
	logger := log.With(zap.String("self", l.self.Name))

	for {
		if l.watchCtx.ShouldStop() {
			logger.Warn("stop watching leader because of server is closed")
			return
		}

		if wait != waitReasonNoWait {
			logger.Warn("sleep a while during watch", zap.String("wait-reason", wait))
			time.Sleep(WatchLeaderFailInterval)
			wait = waitReasonNoWait
		}

		// check whether leader exists.
		leaderResp, err := l.self.GetLeader()
		if err != nil {
			logger.Error("fail to get leader", zap.Error(err))
			wait = waitReasonFailEtcd
			continue
		}

		etcdLeaderID := l.watchCtx.EtcdLeaderID()
		if leaderResp.Leader == nil {
			// Leader does not exist.
			// A new leader should be elected and the etcd leader should be made the new leader.
			if l.self.ID == etcdLeaderID {
				// campaign the leader and block until leader changes.
				if err := l.self.CampaignAndKeepLeader(); err != nil {
					logger.Error("fail to campaign and keep leader", zap.Error(err))
					wait = waitReasonFailEtcd
				} else {
					logger.Info("stop keeping leader")
				}
				continue
			}

			// for other nodes that is not etcd leader, just wait for the new leader elected.
			wait = waitReasonElectLeader
		} else {
			// Leader does exist.
			// A new leader should be elected (the leader should be reset by the current leader itself) if the leader is
			// not the etcd leader.
			if etcdLeaderID == leaderResp.Leader.Id {
				// watch the leader and block until leader changes.
				if err := l.self.WaitForLeaderChange(); err != nil {
					logger.Error("fail to wait for leader change", zap.Error(err))
				} else {
					logger.Info("leader changes")
				}
				continue
			}

			// the leader is not etcd leader and this node is leader so reset it.
			if leaderResp.Leader.Id == l.self.ID {
				if err := l.self.ResetLeader(); err != nil {
					logger.Error("fail to reset leader", zap.Error(err))
					wait = waitReasonFailEtcd
				}
				continue
			}

			// the leader is not etcd leader and this node is not the leader so just wait a moment and check leader again.
			wait = waitReasonResetLeader
		}
	}
}
