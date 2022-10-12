// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package eventdispatch

import (
	"context"
	"sync"

	"github.com/CeresDB/ceresdbproto/pkg/metaeventpb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/service"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type DispatchImpl struct {
	conns sync.Map
}

func NewDispatchImpl() *DispatchImpl {
	return &DispatchImpl{}
}

func (e *DispatchImpl) OpenShard(ctx context.Context, addr string, request *OpenShardRequest) error {
	client, err := e.getMetaEventClient(ctx, addr)
	if err != nil {
		return err
	}
	resp, err := client.OpenShard(ctx, &metaeventpb.OpenShardRequest{
		Shard: cluster.ConvertShardsInfoToPB(request.Shard),
	})
	if err != nil {
		return errors.WithMessage(err, "open shard")
	}
	if resp.GetHeader().Code != 0 {
		return errors.Errorf("failed to open shard, err:%s", resp.GetHeader().GetError())
	}
	return nil
}

func (e *DispatchImpl) CloseShard(ctx context.Context, addr string, request *CloseShardRequest) error {
	client, err := e.getMetaEventClient(ctx, addr)
	if err != nil {
		return err
	}
	resp, err := client.CloseShard(ctx, &metaeventpb.CloseShardRequest{
		ShardId: request.ShardID,
	})
	if err != nil {
		return errors.WithMessage(err, "close shard")
	}
	if resp.GetHeader().Code != 0 {
		return errors.Errorf("failed to close shard, err:%s", resp.GetHeader().GetError())
	}
	return nil
}

func (e *DispatchImpl) CreateTableOnShard(ctx context.Context, addr string, request *CreateTableOnShardRequest) error {
	client, err := e.getMetaEventClient(ctx, addr)
	if err != nil {
		return err
	}
	resp, err := client.CreateTableOnShard(ctx, convertCreateTableOnShardRequestToPB(request))
	if err != nil {
		return errors.WithMessage(err, "create table on shard")
	}
	if resp.GetHeader().Code != 0 {
		return errors.Errorf("failed to create table on shard, err:%s", resp.GetHeader().GetError())
	}
	return nil
}

func (e *DispatchImpl) DropTableOnShard(ctx context.Context, addr string, request *DropTableOnShardRequest) error {
	client, err := e.getMetaEventClient(ctx, addr)
	if err != nil {
		return err
	}
	resp, err := client.DropTableOnShard(ctx, convertDropTableOnShardRequestToPB(request))
	if err != nil {
		return errors.WithMessage(err, "drop table on shard")
	}
	if resp.GetHeader().Code != 0 {
		return errors.Errorf("failed to drop table on shard, err:%s", resp.GetHeader().GetError())
	}
	return nil
}

func (e *DispatchImpl) getGrpcClient(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	client, ok := e.conns.Load(addr)
	if !ok {
		cc, err := service.GetClientConn(ctx, addr)
		if err != nil {
			return nil, err
		}
		client = cc
		e.conns.Store(addr, cc)
	}
	return client.(*grpc.ClientConn), nil
}

func (e *DispatchImpl) getMetaEventClient(ctx context.Context, addr string) (metaeventpb.MetaEventServiceClient, error) {
	client, err := e.getGrpcClient(ctx, addr)
	if err != nil {
		return nil, errors.WithMessagef(err, "get meta event client, addr:%s", addr)
	}
	return metaeventpb.NewMetaEventServiceClient(client), nil
}

func convertCreateTableOnShardRequestToPB(request *CreateTableOnShardRequest) *metaeventpb.CreateTableOnShardRequest {
	return &metaeventpb.CreateTableOnShardRequest{
		UpdateShardInfo: nil,
		TableInfo:       cluster.ConvertTableInfoToPB(request.TableInfo),
		CreateSql:       request.CreateSQL,
	}
}

func convertDropTableOnShardRequestToPB(request *DropTableOnShardRequest) *metaeventpb.DropTableOnShardRequest {
	return &metaeventpb.DropTableOnShardRequest{
		UpdateShardInfo: &metaeventpb.UpdateShardInfo{
			CurrShardInfo: cluster.ConvertShardsInfoToPB(request.ShardInfo),
			PrevVersion:   request.PrevVersion,
		},
		TableInfo: cluster.ConvertTableInfoToPB(request.TableInfo),
	}
}
