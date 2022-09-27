// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package dispatch

import (
	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
)

type EventDispatch interface {
	OpenEvent(ShardIDs []uint32, targetNode string) error

	CloseEvent(ShardIDs []uint32, targetNode string) error

	ChangeRoleEvent(ShardID uint32, shardRole clusterpb.ShardRole, targetNode string) error
}
