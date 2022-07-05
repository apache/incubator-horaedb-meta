// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package grpcservice

import (
	"context"
	"io"
	"time"

	"github.com/CeresDB/ceresdbproto/pkg/metapb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"go.uber.org/zap"
)

type Server struct {
	metapb.UnimplementedCeresmetaRpcServiceServer

	opTimeout time.Duration
	h         Handler
}

func NewService(opTimeout time.Duration, h Handler) *Server {
	return &Server{
		opTimeout: opTimeout,
		h:         h,
	}
}

type HeartbeatSender interface {
	Send(response *metapb.NodeHeartbeatResponse) error
}
type Handler interface {
	RegisterHeartbeatSender(ctx context.Context, sender HeartbeatSender) error
	HandleHeartbeat(ctx context.Context, req *metapb.NodeHeartbeatRequest) error
}

func (s *Server) NodeHeartbeat(heartbeatSrv metapb.CeresmetaRpcService_NodeHeartbeatServer) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.h.RegisterHeartbeatSender(ctx, heartbeatSrv); err != nil {
		return ErrRegisterHeartbeatSender.WithCause(err)
	}

	for {
		req, err := heartbeatSrv.Recv()
		if err == io.EOF {
			log.Warn("receive EOF and exit the heartbeat loop")
			return nil
		}
		if err != nil {
			return ErrRecvHeartbeat.WithCause(err)
		}

		// TODO: It is better to process this request background.
		func() {
			ctx1, cancel := context.WithTimeout(ctx, s.opTimeout)
			defer cancel()
			err := s.h.HandleHeartbeat(ctx1, req)
			if err != nil {
				log.Error("fail to handle heartbeat", zap.Any("heartbeat", req), zap.Error(err))
			}

			log.Debug("succeed in handling heartbeat", zap.Any("heartbeat", req))
		}()
	}
}
