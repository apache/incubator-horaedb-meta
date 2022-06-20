package config

import "github.com/CeresDB/ceresmeta/codeerr"

var (
	ErrInvalidPeerUrl     = codeerr.NewCodeErrorWrapper(codeerr.InvalidParams, "invalid peers url")
	ErrInvalidCommandArgs = codeerr.NewCodeErrorWrapper(codeerr.InvalidParams, "invalid command arguments")
	ErrRetrieveHostname   = codeerr.NewCodeErrorWrapper(codeerr.Internal, "fail to retrieve local hostname")
)
