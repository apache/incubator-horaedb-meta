package dispatch

import "github.com/CeresDB/ceresdbproto/pkg/clusterpb"

type EventDispatchImpl struct {
}

func (d *EventDispatchImpl) SendOpenEvent(ShardIDs []uint32, targetNode string) (bool, error) {
	return false, nil
}

func (d *EventDispatchImpl) SendCloseEvent(ShardIDs []uint32, targetNode string) (bool, error) {
	return false, nil
}

func (d *EventDispatchImpl) SendChangeRoleEvent(ShardID uint32, shardRole clusterpb.ShardRole, targetNode string) (bool, error) {
	return false, nil
}
