// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package dispatch

import (
	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
)

type EventDispatchImpl struct{}

func NewEventDispatchImpl() *EventDispatchImpl {
	return &EventDispatchImpl{}
}

func (d *EventDispatchImpl) OpenShards(_ []uint32, _ string) error {
	// TODO: impl later
	return nil
}

func (d *EventDispatchImpl) CloseShards(_ []uint32, _ string) error {
	// TODO: impl later
	return nil
}

func (d *EventDispatchImpl) ChangeShardRole(_ uint32, _ clusterpb.ShardRole, _ string) error {
	// TODO: impl later
	return nil
}
