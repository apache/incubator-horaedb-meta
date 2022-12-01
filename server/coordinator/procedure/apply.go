// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	eventApplyCloseShard = "EventApplyCloseShard"
	eventApplyOpenShard  = "EventApplyOpenShard"
	eventApplyFinish     = "EventApplyFinish"

	stateApplyBegin      = "StateApplyBegin"
	stateApplyCloseShard = "StateApplyCloseShard"
	stateApplyOpenShard  = "StateApplyOpenShard"
	stateApplyFinish     = "stateApplyFinish"
)

var (
	applyEvents = fsm.Events{
		{Name: eventApplyCloseShard, Src: []string{stateApplyBegin}, Dst: stateApplyCloseShard},
		{Name: eventApplyOpenShard, Src: []string{stateApplyCloseShard}, Dst: stateApplyOpenShard},
		{Name: eventApplyFinish, Src: []string{stateApplyOpenShard}, Dst: stateApplyFinish},
	}
	applyCallbacks = fsm.Callbacks{
		eventApplyCloseShard: applyCloseShardCallback,
		eventApplyOpenShard:  applyOpenShardCallback,
		eventApplyFinish:     applyFinishCallback,
	}
)

type ApplyCallbackRequest struct {
	ctx context.Context

	nodeName                   string
	shardsNeedToReopen         []cluster.ShardInfo
	shardsNeedToCloseAndReopen []cluster.ShardInfo
	shardsNeedToClose          []cluster.ShardInfo

	dispatch eventdispatch.Dispatch
}

// ApplyProcedure used for apply the latest cluster topology to CeresDB node when metadata version is not equal to CeresDB node version.
type ApplyProcedure struct {
	id       uint64
	fsm      *fsm.FSM
	dispatch eventdispatch.Dispatch
	storage  Storage

	nodeName                   string
	shardsNeedToReopen         []cluster.ShardInfo
	shardsNeedToCloseAndReopen []cluster.ShardInfo
	shardsNeedToClose          []cluster.ShardInfo

	// Protect the state.
	lock  sync.RWMutex
	state State
}

type ApplyProcedureRequest struct {
	Dispatch                   eventdispatch.Dispatch
	ID                         uint64
	NodeName                   string
	ShardsNeedToReopen         []cluster.ShardInfo
	ShardsNeedToCloseAndReopen []cluster.ShardInfo
	ShardNeedToClose           []cluster.ShardInfo
	Storage                    Storage
}

func NewApplyProcedure(request ApplyProcedureRequest) Procedure {
	return &ApplyProcedure{
		id:                         request.ID,
		fsm:                        fsm.NewFSM(stateApplyBegin, applyEvents, applyCallbacks),
		nodeName:                   request.NodeName,
		shardsNeedToReopen:         request.ShardsNeedToReopen,
		shardsNeedToClose:          request.ShardNeedToClose,
		shardsNeedToCloseAndReopen: request.ShardsNeedToCloseAndReopen,
		dispatch:                   request.Dispatch,
		storage:                    request.Storage,
	}
}

func (p *ApplyProcedure) ID() uint64 {
	return p.id
}

func (p *ApplyProcedure) Typ() Typ {
	return Apply
}

func (p *ApplyProcedure) Start(ctx context.Context) error {
	log.Info("apply procedure start", zap.Uint64("procedureID", p.id))

	p.updateStateWithLock(StateRunning)

	applyCallbackRequest := &ApplyCallbackRequest{
		ctx:                        ctx,
		nodeName:                   p.nodeName,
		shardsNeedToReopen:         p.shardsNeedToReopen,
		shardsNeedToCloseAndReopen: p.shardsNeedToCloseAndReopen,
		shardsNeedToClose:          p.shardsNeedToClose,
		dispatch:                   p.dispatch,
	}

	for {
		switch p.fsm.Current() {
		// TODO: add double check shard info state.
		case stateApplyBegin:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "apply procedure persist")
			}
			if err := p.fsm.Event(eventApplyCloseShard, applyCallbackRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessage(err, "apply procedure close shard")
			}
		case stateApplyCloseShard:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "apply procedure persist")
			}
			if err := p.fsm.Event(eventApplyOpenShard, applyCallbackRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessage(err, "apply procedure open shard")
			}
		case stateApplyOpenShard:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "apply procedure persist")
			}
			if err := p.fsm.Event(eventApplyFinish, applyCallbackRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessage(err, "apply procedure oepn new leader")
			}
		case stateApplyFinish:
			p.updateStateWithLock(StateFinished)
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "apply procedure persist")
			}
			return nil
		}
	}
}

func (p *ApplyProcedure) Cancel(_ context.Context) error {
	p.updateStateWithLock(StateCancelled)
	return nil
}

func (p *ApplyProcedure) State() State {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.state
}

func (p *ApplyProcedure) updateStateWithLock(state State) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.state = state
}

func applyCloseShardCallback(event *fsm.Event) {
	request, err := getRequestFromEvent[*ApplyCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}

	shardsToClose := []cluster.ShardInfo{}
	shardsToClose = append(shardsToClose, request.shardsNeedToCloseAndReopen...)
	shardsToClose = append(shardsToClose, request.shardsNeedToClose...)

	g := new(errgroup.Group)
	for _, shardInfo := range shardsToClose {
		shardInfo := shardInfo
		g.Go(func() error {
			log.Info("close shard begin", zap.Uint32("shardID", uint32(shardInfo.ID)))
			if err := request.dispatch.CloseShard(request.ctx, request.nodeName, eventdispatch.CloseShardRequest{ShardID: uint32(shardInfo.ID)}); err != nil {
				log.Error("close shard failed", zap.Uint32("shardID", uint32(shardInfo.ID)))
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		cancelEventWithLog(event, err, "close shard failed")
		return
	}
}

func applyOpenShardCallback(event *fsm.Event) {
	request, err := getRequestFromEvent[*ApplyCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}

	shardsToOpen := []cluster.ShardInfo{}
	shardsToOpen = append(shardsToOpen, request.shardsNeedToCloseAndReopen...)
	shardsToOpen = append(shardsToOpen, request.shardsNeedToReopen...)

	g := new(errgroup.Group)
	for _, shardInfo := range shardsToOpen {
		shardInfo := shardInfo
		g.Go(func() error {
			log.Info("open shard begin", zap.Uint32("shardID", uint32(shardInfo.ID)))
			if err := request.dispatch.OpenShard(request.ctx, request.nodeName, eventdispatch.OpenShardRequest{Shard: shardInfo}); err != nil {
				log.Error("open shard failed", zap.Uint32("shardID", uint32(shardInfo.ID)))
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		cancelEventWithLog(event, err, "open shard failed")
		return
	}
}

func applyFinishCallback(event *fsm.Event) {
	request, err := getRequestFromEvent[*ApplyCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}

	log.Info("apply procedure finish", zap.String("targetNode", request.nodeName))
}

func (p *ApplyProcedure) persist(ctx context.Context) error {
	meta, err := p.convertToMeta()
	if err != nil {
		return errors.WithMessage(err, "convert to meta")
	}
	err = p.storage.CreateOrUpdate(ctx, meta)
	if err != nil {
		return errors.WithMessage(err, "createOrUpdate in storage")
	}
	return nil
}

type ApplyProcedurePersistRawData struct {
	ID       uint64
	FsmState string
	State    State

	NodeName                   string
	ShardsNeedToReopen         []cluster.ShardInfo
	ShardsNeedToCloseAndReopen []cluster.ShardInfo
	ShardsNeedToClose          []cluster.ShardInfo
}

func (p *ApplyProcedure) convertToMeta() (Meta, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	rawData := ApplyProcedurePersistRawData{
		ID:                         p.id,
		FsmState:                   p.fsm.Current(),
		State:                      p.state,
		NodeName:                   p.nodeName,
		ShardsNeedToReopen:         p.shardsNeedToReopen,
		ShardsNeedToCloseAndReopen: p.shardsNeedToCloseAndReopen,
		ShardsNeedToClose:          p.shardsNeedToClose,
	}
	rawDataBytes, err := json.Marshal(rawData)
	if err != nil {
		return Meta{}, ErrEncodeRawData.WithCausef("marshal raw data, procedureID:%d, err:%v", p.id, err)
	}

	meta := Meta{
		ID:    p.id,
		Typ:   Apply,
		State: p.state,

		RawData: rawDataBytes,
	}

	return meta, nil
}
