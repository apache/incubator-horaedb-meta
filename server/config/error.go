package config

import (
	coderr2 "github.com/CeresDB/ceresmeta/pkg/coderr"
)

var (
	ErrInvalidPeerURL     = coderr2.NewCodeErrorWrapper(coderr2.InvalidParams, "invalid peers url")
	ErrInvalidCommandArgs = coderr2.NewCodeErrorWrapper(coderr2.InvalidParams, "invalid command arguments")
	ErrRetrieveHostname   = coderr2.NewCodeErrorWrapper(coderr2.Internal, "fail to retrieve local hostname")
)
