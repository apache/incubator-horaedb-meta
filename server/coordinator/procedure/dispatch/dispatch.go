package dispatch

import "github.com/CeresDB/ceresdbproto/pkg/clusterpb"

type EventDispatch interface {
	SendOpenEvent(ShardIDs []uint32, targetNode string) (bool, error)

	SendCloseEvent(ShardIDs []uint32, targetNode string) (bool, error)

	SendChangeRoleEvent(ShardID uint32, shardRole clusterpb.ShardRole, targetNode string) (bool, error)
}
