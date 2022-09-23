// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package dispatch

import (
	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
)

type EventDispatchImpl struct{}

func NewEventDispatchImpl() *EventDispatchImpl {
	return &EventDispatchImpl{}
}

func (d *EventDispatchImpl) SendOpenEvent(shardIDs []uint32, targetNode string) (bool, error) {
	return false, nil
}

func (d *EventDispatchImpl) SendCloseEvent(shardIDs []uint32, targetNode string) (bool, error) {
	return false, nil
}

func (d *EventDispatchImpl) SendChangeRoleEvent(shardID uint32, shardRole clusterpb.ShardRole, targetNode string) (bool, error) {
	return false, nil
}
