// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package grpcservice

import (
	"context"
	"crypto/tls"
	"net/url"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// GetClientConn returns a gRPC client connection.
func GetClientConn(ctx context.Context, addr string, tlsCfg *tls.Config, do ...grpc.DialOption) (*grpc.ClientConn, error) {
	var creds credentials.TransportCredentials
	if tlsCfg != nil {
		creds = credentials.NewTLS(tlsCfg)
	} else {
		creds = insecure.NewCredentials()
	}

	opt := grpc.WithTransportCredentials(creds)

	u, err := url.Parse(addr)
	if err != nil {
		return nil, ErrParseURL.WithCause(err)
	}

	cc, err := grpc.DialContext(ctx, u.Host, append(do, opt)...)
	if err != nil {
		return nil, ErrGRPCDial.WithCause(err)
	}
	return cc, nil
}
