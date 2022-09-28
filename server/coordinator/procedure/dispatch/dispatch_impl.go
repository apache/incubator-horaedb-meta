// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package dispatch

import (
	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
)

type EventDispatchImpl struct{}

func NewEventDispatchImpl() *EventDispatchImpl {
	return &EventDispatchImpl{}
}

func (d *EventDispatchImpl) OpenShards(shardIDs []uint32, targetNode string) error {
	// TODO: impl later
	return nil
}

func (d *EventDispatchImpl) CloseShards(shardIDs []uint32, targetNode string) error {
	// TODO: impl later
	return nil
}

func (d *EventDispatchImpl) ChangeShardRole(shardID uint32, shardRole clusterpb.ShardRole, targetNode string) error {
	// TODO: impl later
	return nil
}
