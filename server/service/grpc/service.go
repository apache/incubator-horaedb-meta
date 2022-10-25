// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package grpc

import (
	"context"
	"sync"
	"time"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresdbproto/pkg/commonpb"
	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/coderr"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/member"
	"go.uber.org/zap"
)

type Service struct {
	metaservicepb.UnimplementedCeresmetaRpcServiceServer
	opTimeout time.Duration
	h         Handler

	// Store as map[string]*grpc.ClientConn
	// TODO: remove unavailable connection
	conns sync.Map
}

func NewService(opTimeout time.Duration, h Handler) *Service {
	return &Service{
		opTimeout: opTimeout,
		h:         h,
	}
}

// Handler is needed by grpc service to process the requests.
type Handler interface {
	GetClusterManager() cluster.Manager
	GetLeader(ctx context.Context) (*member.GetLeaderResp, error)
	GetProcedureFactory() *procedure.Factory
	GetProcedureManager() procedure.Manager

	// TODO: define the methods for handling other grpc requests.
}

// NodeHeartbeat implements gRPC CeresmetaServer.
func (s *Service) NodeHeartbeat(ctx context.Context, req *metaservicepb.NodeHeartbeatRequest) (*metaservicepb.NodeHeartbeatResponse, error) {
	ceresmetaClient, err := s.getForwardedCeresmetaClient(ctx)
	if err != nil {
		return &metaservicepb.NodeHeartbeatResponse{Header: responseHeader(err, "grpc heartbeat")}, nil
	}

	// Forward request to the leader.
	if ceresmetaClient != nil {
		return ceresmetaClient.NodeHeartbeat(ctx, req)
	}

	err = s.h.GetClusterManager().RegisterNode(ctx, req.GetHeader().GetClusterName(), req.GetInfo())
	if err != nil {
		return &metaservicepb.NodeHeartbeatResponse{Header: responseHeader(err, "grpc heartbeat")}, nil
	}

	return &metaservicepb.NodeHeartbeatResponse{
		Header: okResponseHeader(),
	}, nil
}

// AllocSchemaID implements gRPC CeresmetaServer.
func (s *Service) AllocSchemaID(ctx context.Context, req *metaservicepb.AllocSchemaIdRequest) (*metaservicepb.AllocSchemaIdResponse, error) {
	ceresmetaClient, err := s.getForwardedCeresmetaClient(ctx)
	if err != nil {
		return &metaservicepb.AllocSchemaIdResponse{Header: responseHeader(err, "grpc alloc schema id")}, nil
	}

	// Forward request to the leader.
	if ceresmetaClient != nil {
		return ceresmetaClient.AllocSchemaID(ctx, req)
	}

	schemaID, _, err := s.h.GetClusterManager().AllocSchemaID(ctx, req.GetHeader().GetClusterName(), req.GetName())
	if err != nil {
		return &metaservicepb.AllocSchemaIdResponse{Header: responseHeader(err, "grpc alloc schema id")}, nil
	}

	return &metaservicepb.AllocSchemaIdResponse{
		Header: okResponseHeader(),
		Name:   req.GetName(),
		Id:     schemaID,
	}, nil
}

// GetTablesOfShards implements gRPC CeresmetaServer.
func (s *Service) GetTablesOfShards(ctx context.Context, req *metaservicepb.GetTablesOfShardsRequest) (*metaservicepb.GetTablesOfShardsResponse, error) {
	ceresmetaClient, err := s.getForwardedCeresmetaClient(ctx)
	if err != nil {
		return &metaservicepb.GetTablesOfShardsResponse{Header: responseHeader(err, "grpc get tables of shards")}, nil
	}

	// Forward request to the leader.
	if ceresmetaClient != nil {
		return ceresmetaClient.GetTablesOfShards(ctx, req)
	}

	tables, err := s.h.GetClusterManager().GetTables(ctx, req.GetHeader().GetClusterName(), req.GetHeader().GetNode(), req.GetShardIds())
	if err != nil {
		return &metaservicepb.GetTablesOfShardsResponse{Header: responseHeader(err, "grpc get tables of shards")}, nil
	}

	return convertToGetTablesOfShardsResponse(tables), nil
}

// CreateTable implements gRPC CeresmetaServer.
func (s *Service) CreateTable(ctx context.Context, req *metaservicepb.CreateTableRequest) (*metaservicepb.CreateTableResponse, error) {
	clusterManager := s.h.GetClusterManager()
	factory := s.h.GetProcedureFactory()
	manager := s.h.GetProcedureManager()
	c, err := clusterManager.GetCluster(ctx, req.GetHeader().GetClusterName())
	if err != nil {
		log.Error("fail to create table", zap.Error(err))
		return &metaservicepb.CreateTableResponse{Header: responseHeader(err, "create table")}, nil
	}

	errorCh := make(chan error, 1)
	resultCh := make(chan *cluster.CreateTableResult, 1)

	onSucceeded := func(ret *cluster.CreateTableResult) error {
		resultCh <- ret
		return nil
	}
	onFailed := func(err error) error {
		errorCh <- err
		return nil
	}

	procedure, err := factory.CreateCreateTableProcedure(ctx, &procedure.CreateTableRequest{
		Cluster:     c,
		SourceReq:   req,
		OnSucceeded: onSucceeded,
		OnFailed:    onFailed,
	})
	if err != nil {
		log.Error("fail to create table", zap.Error(err))
		return &metaservicepb.CreateTableResponse{Header: responseHeader(err, "create table")}, nil
	}
	err = manager.Submit(ctx, procedure)
	if err != nil {
		log.Error("fail to create table, manager submit procedure", zap.Error(err))
		return &metaservicepb.CreateTableResponse{Header: responseHeader(err, "create table")}, nil
	}

	select {
	case ret := <-resultCh:
		return &metaservicepb.CreateTableResponse{
			Header: okResponseHeader(),
			CreatedTable: &metaservicepb.TableInfo{
				Id:         ret.Table.GetID(),
				Name:       ret.Table.GetName(),
				SchemaId:   ret.Table.GetSchemaID(),
				SchemaName: ret.Table.GetSchemaName(),
			},
			ShardInfo: &metaservicepb.ShardInfo{
				Id:      ret.ShardVersion.ShardID,
				Role:    clusterpb.ShardRole_LEADER,
				Version: ret.ShardVersion.CurrVersion,
			},
		}, nil
	case err = <-errorCh:
		return &metaservicepb.CreateTableResponse{Header: responseHeader(err, "create table")}, nil
	}
}

// DropTable implements gRPC CeresmetaServer.
func (s *Service) DropTable(ctx context.Context, req *metaservicepb.DropTableRequest) (*metaservicepb.DropTableResponse, error) {
	clusterManager := s.h.GetClusterManager()
	factory := s.h.GetProcedureFactory()
	manager := s.h.GetProcedureManager()
	c, err := clusterManager.GetCluster(ctx, req.GetHeader().GetClusterName())
	if err != nil {
		log.Error("fail to drop table", zap.Error(err))
		return &metaservicepb.DropTableResponse{Header: responseHeader(err, "drop table")}, nil
	}

	errorCh := make(chan error, 1)
	resultCh := make(chan *cluster.TableInfo, 1)

	onSucceeded := func(ret *cluster.TableInfo) error {
		resultCh <- ret
		return nil
	}
	onFailed := func(err error) error {
		errorCh <- err
		return nil
	}

	procedure, err := factory.CreateDropTableProcedure(ctx, &procedure.DropTableRequest{
		Cluster:     c,
		SourceReq:   req,
		OnSucceeded: onSucceeded,
		OnFailed:    onFailed,
	})
	if err != nil {
		log.Error("fail to drop table", zap.Error(err))
		return &metaservicepb.DropTableResponse{Header: responseHeader(err, "drop table")}, nil
	}
	err = manager.Submit(ctx, procedure)
	if err != nil {
		log.Error("fail to drop table, manager submit procedure", zap.Error(err))
		return &metaservicepb.DropTableResponse{Header: responseHeader(err, "drop table")}, nil
	}

	select {
	case ret := <-resultCh:
		return &metaservicepb.DropTableResponse{
			Header:       okResponseHeader(),
			DroppedTable: cluster.ConvertTableInfoToPB(ret),
		}, nil
	case err = <-errorCh:
		return &metaservicepb.DropTableResponse{Header: responseHeader(err, "drop table")}, nil
	}
}

// RouteTables implements gRPC CeresmetaServer.
func (s *Service) RouteTables(ctx context.Context, req *metaservicepb.RouteTablesRequest) (*metaservicepb.RouteTablesResponse, error) {
	ceresmetaClient, err := s.getForwardedCeresmetaClient(ctx)
	if err != nil {
		return &metaservicepb.RouteTablesResponse{Header: responseHeader(err, "grpc routeTables")}, nil
	}

	// Forward request to the leader.
	if ceresmetaClient != nil {
		return ceresmetaClient.RouteTables(ctx, req)
	}

	routeTableResult, err := s.h.GetClusterManager().RouteTables(ctx, req.GetHeader().GetClusterName(), req.GetSchemaName(), req.GetTableNames())
	if err != nil {
		return &metaservicepb.RouteTablesResponse{Header: responseHeader(err, "grpc routeTables")}, nil
	}

	return convertRouteTableResult(routeTableResult), nil
}

// GetNodes implements gRPC CeresmetaServer.
func (s *Service) GetNodes(ctx context.Context, req *metaservicepb.GetNodesRequest) (*metaservicepb.GetNodesResponse, error) {
	ceresmetaClient, err := s.getForwardedCeresmetaClient(ctx)
	if err != nil {
		return &metaservicepb.GetNodesResponse{Header: responseHeader(err, "grpc get nodes")}, nil
	}

	// Forward request to the leader.
	if ceresmetaClient != nil {
		return ceresmetaClient.GetNodes(ctx, req)
	}

	nodesResult, err := s.h.GetClusterManager().GetNodeShards(ctx, req.GetHeader().GetClusterName())
	if err != nil {
		log.Error("fail to get nodes", zap.Error(err))
		return &metaservicepb.GetNodesResponse{Header: responseHeader(err, "grpc get nodes")}, nil
	}

	return convertToGetNodesResponse(nodesResult), nil
}

func convertToGetTablesOfShardsResponse(shardTables map[uint32]*cluster.ShardTables) *metaservicepb.GetTablesOfShardsResponse {
	tablesByShard := make(map[uint32]*metaservicepb.TablesOfShard, len(shardTables))
	for id, shardTable := range shardTables {
		tables := make([]*metaservicepb.TableInfo, 0, len(shardTable.Tables))
		for _, table := range shardTable.Tables {
			tables = append(tables, cluster.ConvertTableInfoToPB(table))
		}
		tablesByShard[id] = &metaservicepb.TablesOfShard{
			ShardInfo: cluster.ConvertShardsInfoToPB(shardTable.Shard),
			Tables:    tables,
		}
	}
	return &metaservicepb.GetTablesOfShardsResponse{
		Header:        okResponseHeader(),
		TablesByShard: tablesByShard,
	}
}

func convertRouteTableResult(routeTablesResult *cluster.RouteTablesResult) *metaservicepb.RouteTablesResponse {
	entries := make(map[string]*metaservicepb.RouteEntry, len(routeTablesResult.RouteEntries))
	for tableName, entry := range routeTablesResult.RouteEntries {
		nodeShards := make([]*metaservicepb.NodeShard, 0, len(entry.NodeShards))
		for _, nodeShard := range entry.NodeShards {
			nodeShards = append(nodeShards, &metaservicepb.NodeShard{
				Endpoint: nodeShard.Endpoint,
				ShardInfo: &metaservicepb.ShardInfo{
					Id:   nodeShard.ShardInfo.ID,
					Role: nodeShard.ShardInfo.Role,
				},
			})
		}

		entries[tableName] = &metaservicepb.RouteEntry{
			Table:      cluster.ConvertTableInfoToPB(entry.Table),
			NodeShards: nodeShards,
		}
	}

	return &metaservicepb.RouteTablesResponse{
		Header:                 okResponseHeader(),
		ClusterTopologyVersion: routeTablesResult.Version,
		Entries:                entries,
	}
}

func convertToGetNodesResponse(nodesResult *cluster.GetNodeShardsResult) *metaservicepb.GetNodesResponse {
	nodeShards := make([]*metaservicepb.NodeShard, 0, len(nodesResult.NodeShards))
	for _, nodeShard := range nodesResult.NodeShards {
		nodeShards = append(nodeShards, &metaservicepb.NodeShard{
			Endpoint:  nodeShard.Endpoint,
			ShardInfo: cluster.ConvertShardsInfoToPB(nodeShard.ShardInfo),
		})
	}
	return &metaservicepb.GetNodesResponse{
		Header:                 okResponseHeader(),
		ClusterTopologyVersion: nodesResult.ClusterTopologyVersion,
		NodeShards:             nodeShards,
	}
}

func okResponseHeader() *commonpb.ResponseHeader {
	return responseHeader(nil, "")
}

func responseHeader(err error, msg string) *commonpb.ResponseHeader {
	if err == nil {
		return &commonpb.ResponseHeader{Code: coderr.Ok, Error: msg}
	}

	code, ok := coderr.GetCauseCode(err)
	if ok {
		return &commonpb.ResponseHeader{Code: uint32(code), Error: msg + err.Error()}
	}

	return &commonpb.ResponseHeader{Code: coderr.Internal, Error: msg + err.Error()}
}
