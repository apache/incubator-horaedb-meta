// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package http

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/member"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type ForwardClient struct {
	member *member.Member
	client *http.Client
	port   int
}

func NewForwardClient(member *member.Member, port int) *ForwardClient {
	return &ForwardClient{
		member: member,
		client: getForwardedHTTPClient(),
		port:   port,
	}
}

func getForwardedHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout: 10 * time.Second,
		},
	}
}

func (s *ForwardClient) getForwardedAddr(ctx context.Context) (string, bool, error) {
	member, err := s.member.GetLeader(ctx)
	if err != nil {
		return "", false, errors.WithMessage(err, "get forwarded addr")
	}
	if member.IsLocal {
		return "", true, nil
	}
	leaderAddr := strings.Split(member.Leader.GetEndpoint(), ":")
	leaderAddr[2] = strconv.Itoa(s.port)
	return strings.Join(leaderAddr, ":"), false, nil
}

func (s *ForwardClient) forwardToLeader(req *http.Request) (*http.Response, bool, error) {
	addr, isLeader, err := s.getForwardedAddr(req.Context())
	if err != nil {
		log.Error("get forward addr failed", zap.Error(err))
		return &http.Response{}, false, err
	}
	if isLeader {
		return &http.Response{}, true, nil
	}

	// Update remote host
	req.RequestURI = ""
	if req.TLS == nil {
		req.URL.Scheme = "http"
	} else {
		req.URL.Scheme = "https"
	}
	req.URL.Host = addr

	resp, err := s.client.Do(req)
	if err != nil {
		log.Error("forward client send request failed", zap.Error(err))
		return &http.Response{}, false, err
	}

	return resp, false, nil
}
