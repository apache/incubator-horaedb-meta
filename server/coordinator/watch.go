package coordinator

import (
	"context"
	"fmt"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/storage"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"strconv"
	"strings"
)

type ShardEventType uint64

const (
	shardPath                  = "shards"
	eventDelete ShardEventType = 0
	eventPut                   = 1
)

type ShardWatch struct {
	rootPath      string
	etcdClient    *clientv3.Client
	eventCallback func(shardID storage.ShardID, nodeName string, eventType ShardEventType) error
}

func (w *ShardWatch) registerCallback(eventCallback func(shardID storage.ShardID, nodeName string, eventType ShardEventType) error) {
	w.eventCallback = eventCallback
}

func (w *ShardWatch) registerWatch(ctx context.Context) error {
	shardsKeyPath := strings.Join([]string{w.rootPath, shardPath}, "/")
	log.Info("register shard watch", zap.String("watchPath", shardsKeyPath))
	go func() {
		respChan := w.etcdClient.Watch(ctx, shardsKeyPath, clientv3.WithPrefix(), clientv3.WithPrevKV())
		for resp := range respChan {
			for _, event := range resp.Events {
				if err := w.processEvent(event); err != nil {
					log.Error("process event", zap.Error(err))
				}
			}
		}
	}()
	return nil
}

func (w *ShardWatch) processEvent(event *clientv3.Event) error {
	switch event.Type {
	// DELETE means shard lease is expired,
	case mvccpb.DELETE:
		pathList := strings.Split(string(event.Kv.Key), "/")
		shardID, err := strconv.ParseUint(pathList[len(pathList)-1], 10, 64)
		if err != nil {
			return err
		}
		oldLeader := string(event.PrevKv.Value)
		log.Info("receive delete event", zap.String("preKV", fmt.Sprintf("%v", event.PrevKv)), zap.String("event", fmt.Sprintf("%v", event)), zap.Uint64("shardID", shardID), zap.String("oldLeader", oldLeader))
		return w.eventCallback(storage.ShardID(shardID), oldLeader, eventDelete)
	// PUT means new leader is elected.
	case mvccpb.PUT:
		pathList := strings.Split(string(event.Kv.Key), "/")
		shardID, err := strconv.ParseUint(pathList[len(pathList)-1], 10, 64)
		if err != nil {
			return err
		}
		newLeader := string(event.Kv.Value)
		log.Info("receive put event", zap.String("event", fmt.Sprintf("%v", event)), zap.Uint64("shardID", shardID), zap.String("oldLeader", newLeader))
		return w.eventCallback(storage.ShardID(shardID), newLeader, eventPut)
	}
	return nil
}
