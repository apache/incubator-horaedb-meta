package config

import "github.com/CeresDB/ceresmeta/coderr"

var (
	ErrInvalidPeerURL     = coderr.NewCodeErrorWrapper(coderr.InvalidParams, "invalid peers url")
	ErrInvalidCommandArgs = coderr.NewCodeErrorWrapper(coderr.InvalidParams, "invalid command arguments")
	ErrRetrieveHostname   = coderr.NewCodeErrorWrapper(coderr.Internal, "fail to retrieve local hostname")
)
