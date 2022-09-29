// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package dispatch

import (
	"context"
	"sync"

	"github.com/CeresDB/ceresdbproto/pkg/metaeventpb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/service"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type EventDispatchImpl struct {
	conns sync.Map
}

func NewEventDispatchImpl() *EventDispatchImpl {
	return &EventDispatchImpl{}
}

func (e *EventDispatchImpl) OpenShard(ctx context.Context, addr string, request *OpenShardRequest) error {
	client, err := e.getMetaEventClient(ctx, addr)
	if err != nil {
		return err
	}
	resp, err := client.OpenShard(ctx, &metaeventpb.OpenShardRequest{
		Shard: cluster.ConvertShardsInfo(request.Shard),
	})
	if err != nil {
		return errors.WithMessage(err, "open shards")
	}
	if resp.GetHeader().Code != 0 {
		return errors.Errorf("failed to open shards, err:%s", resp.GetHeader().GetError())
	}
	return nil
}

func (e *EventDispatchImpl) CloseShard(ctx context.Context, addr string, request *CloseShardRequest) error {
	client, err := e.getMetaEventClient(ctx, addr)
	if err != nil {
		return err
	}
	resp, err := client.CloseShard(ctx, &metaeventpb.CloseShardRequest{
		ShardId: request.ShardID,
	})
	if err != nil {
		return errors.WithMessage(err, "close shards")
	}
	if resp.GetHeader().Code != 0 {
		return errors.Errorf("failed to close shards, err:%s", resp.GetHeader().GetError())
	}
	return nil
}

func (e *EventDispatchImpl) CreateTableOnShard(ctx context.Context, addr string, request *CreateTableOnShardRequest) error {
	client, err := e.getMetaEventClient(ctx, addr)
	if err != nil {
		return err
	}
	resp, err := client.CreateTableOnShard(ctx, convertCreateTableOnShardRequest(request))
	if err != nil {
		return errors.WithMessage(err, "create table on shard")
	}
	if resp.GetHeader().Code != 0 {
		return errors.Errorf("failed to create table on shard, err:%s", resp.GetHeader().GetError())
	}
	return nil
}

func (e *EventDispatchImpl) DropTableOnShard(ctx context.Context, addr string, request *DropTableOnShardRequest) error {
	client, err := e.getMetaEventClient(ctx, addr)
	if err != nil {
		return err
	}
	resp, err := client.DropTableOnShard(ctx, convertDropTableOnShardRequest(request))
	if err != nil {
		return errors.WithMessage(err, "drop table on shard")
	}
	if resp.GetHeader().Code != 0 {
		return errors.Errorf("failed to drop table on shard, err:%s", resp.GetHeader().GetError())
	}
	return nil
}

func (e *EventDispatchImpl) getGrpcClient(ctx context.Context, forwardedAddr string) (*grpc.ClientConn, error) {
	client, ok := e.conns.Load(forwardedAddr)
	if !ok {
		cc, err := service.GetClientConn(ctx, forwardedAddr)
		if err != nil {
			return nil, err
		}
		client = cc
		e.conns.Store(forwardedAddr, cc)
	}
	return client.(*grpc.ClientConn), nil
}

func (e *EventDispatchImpl) getMetaEventClient(ctx context.Context, addr string) (metaeventpb.MetaEventServiceClient, error) {
	client, err := e.getGrpcClient(ctx, addr)
	if err != nil {
		return nil, errors.WithMessagef(err, "get meta event client, addr:%s", addr)
	}
	return metaeventpb.NewMetaEventServiceClient(client), nil
}

func convertCreateTableOnShardRequest(request *CreateTableOnShardRequest) *metaeventpb.CreateTableOnShardRequest {
	return &metaeventpb.CreateTableOnShardRequest{
		UpdateShardInfo: nil,
		TableInfo:       cluster.ConvertTableInfo(request.TableInfo),
		CreateSql:       request.CreateSQL,
	}
}

func convertDropTableOnShardRequest(request *DropTableOnShardRequest) *metaeventpb.DropTableOnShardRequest {
	return &metaeventpb.DropTableOnShardRequest{
		UpdateShardInfo: &metaeventpb.UpdateShardInfo{
			CurrShardInfo: cluster.ConvertShardsInfo(request.ShardInfo),
			PrevVersion:   request.PrevVersion,
		},
		TableInfo: cluster.ConvertTableInfo(request.TableInfo),
	}
}
