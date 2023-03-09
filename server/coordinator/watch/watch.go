// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type ShardEventType uint64

const (
	shardPath                  = "shards"
	EventDelete ShardEventType = 0
	EventPut                   = 1
)

// ShardWatch used to watch the distributed lock of shard, and provide the corresponding callback function.
type ShardWatch struct {
	rootPath       string
	etcdClient     *clientv3.Client
	eventCallbacks []func(shardID storage.ShardID, nodeName string, eventType ShardEventType) error

	lock      sync.RWMutex
	isRunning bool
	quit      chan bool
}

func NewWatch(rootPath string, client *clientv3.Client) *ShardWatch {
	return &ShardWatch{
		rootPath:       rootPath,
		etcdClient:     client,
		eventCallbacks: []func(shardID storage.ShardID, nodeName string, eventType ShardEventType) error{},
		quit:           make(chan bool),
	}
}

func (w *ShardWatch) Start(ctx context.Context) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.isRunning {
		return nil
	}

	shardsKeyPath := strings.Join([]string{w.rootPath, shardPath}, "/")

	if err := w.startWatch(ctx, shardsKeyPath); err != nil {
		return errors.WithMessage(err, "etcd register watch failed")
	}

	w.isRunning = true
	return nil
}

func (w *ShardWatch) Stop(ctx context.Context) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.isRunning = false
	w.quit <- true
	return nil
}

func (w *ShardWatch) RegisteringEventCallback(eventCallback func(shardID storage.ShardID, nodeName string, eventType ShardEventType) error) {
	w.eventCallbacks = append(w.eventCallbacks, eventCallback)
}

func (w *ShardWatch) startWatch(ctx context.Context, path string) error {
	log.Info("register shard watch", zap.String("watchPath", path))
	go func() {
		for {
			select {
			case <-w.quit:
				return
			default:
				respChan := w.etcdClient.Watch(ctx, path, clientv3.WithPrefix(), clientv3.WithPrevKV())
				for resp := range respChan {
					for _, event := range resp.Events {
						if err := w.processEvent(event); err != nil {
							log.Error("process event", zap.Error(err))
						}
					}
				}
			}
		}
	}()
	return nil
}

func (w *ShardWatch) processEvent(event *clientv3.Event) error {
	switch event.Type {
	case mvccpb.DELETE:
		pathList := strings.Split(string(event.Kv.Key), "/")
		shardID, err := strconv.ParseUint(pathList[len(pathList)-1], 10, 64)
		if err != nil {
			return err
		}
		oldLeader := string(event.PrevKv.Value)
		log.Info("receive delete event", zap.String("preKV", fmt.Sprintf("%v", event.PrevKv)), zap.String("event", fmt.Sprintf("%v", event)), zap.Uint64("shardID", shardID), zap.String("oldLeader", oldLeader))
		for _, callback := range w.eventCallbacks {
			if err := callback(storage.ShardID(shardID), oldLeader, EventDelete); err != nil {
				return err
			}
		}
	case mvccpb.PUT:
		pathList := strings.Split(string(event.Kv.Key), "/")
		shardID, err := strconv.ParseUint(pathList[len(pathList)-1], 10, 64)
		if err != nil {
			return err
		}
		newLeader := string(event.Kv.Value)
		log.Info("receive put event", zap.String("event", fmt.Sprintf("%v", event)), zap.Uint64("shardID", shardID), zap.String("oldLeader", newLeader))
		for _, callback := range w.eventCallbacks {
			if err := callback(storage.ShardID(shardID), newLeader, EventPut); err != nil {
				return err
			}
		}
	}
	return nil
}
