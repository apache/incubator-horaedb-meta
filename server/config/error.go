// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package config

import (
	"github.com/CeresDB/ceresmeta/pkg/coderr"
)

var (
	ErrInvalidPeerURL     = coderr.NewCodeErrorWrapper(coderr.InvalidParams, "invalid peers url")
	ErrInvalidCommandArgs = coderr.NewCodeErrorWrapper(coderr.InvalidParams, "invalid command arguments")
	ErrRetrieveHostname   = coderr.NewCodeErrorWrapper(coderr.Internal, "retrieve local hostname")
)
