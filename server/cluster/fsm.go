package cluster

import (
	"github.com/looplab/fsm"
)

type ShardOperationType int32

// Shard operation type
const (
	ShardOperationType_Create         ShardOperationType = 0
	ShardOperationType_Delete         ShardOperationType = 1
	ShardOperationType_TransferLeader ShardOperationType = 2
	ShardOperationType_Migrate        ShardOperationType = 3
	ShardOperationType_Split          ShardOperationType = 4
	ShardOperationType_Merge          ShardOperationType = 5

	Event_Create_Prepare = "EventCreatePrepare"
	Event_Create_Failed  = "EventCreateFailed"
	Event_Create_Success = "EventCreateSuccess"
	State_Create_Begin   = "StateCreateBegin"
	State_Create_Waiting = "StateCreateWaiting"
	State_Create_Finish  = "StateCreateFinish"
	State_Create_Failed  = "StateCreateFailed"

	Event_Delete_Prepare = "EventDeletePrepare"
	Event_Delete_Failed  = "EventDeleteFailed"
	Event_Delete_Success = "EventDeleteSuccess"
	State_Delete_Begin   = "StateDeleteBegin"
	State_Delete_Waiting = "StateDeleteWaiting"
	State_Delete_Finish  = "StateDeleteFinish"
	State_Delete_Failed  = "StateDeleteFailed"

	Event_TransferLeader_Prepare = "EventTransferLeaderPrepare"
	Event_TransferLeader_Failed  = "EventTransferLeaderFailed"
	Event_TransferLeader_Success = "EventTransferLeaderSuccess"
	State_TransferLeader_Begin   = "StateTransferLeaderBegin"
	State_TransferLeader_Waiting = "StateTransferLeaderWaiting"
	State_TransferLeader_Finish  = "StateTransferLeaderFinish"
	State_TransferLeader_Failed  = "StateTransferLeaderFailed"

	Event_Migrate_Prepare = "EventMigratePrepare"
	Event_Migrate_Failed  = "EventMigrateFailed"
	Event_Migrate_Success = "EventMigrateSuccess"
	State_Migrate_Begin   = "StateMigrateBegin"
	State_Migrate_Waiting = "StateMigrateWaiting"
	State_Migrate_Finish  = "StateMigrateFinish"
	State_Migrate_Failed  = "StateMigrateFailed"

	Event_Split_Prepare = "EventSplitPrepare"
	Event_Split_Failed  = "EventSplitFailed"
	Event_Split_Success = "EventSplitSuccess"
	State_Split_Begin   = "StateSplitBegin"
	State_Split_Waiting = "StateSplitWaiting"
	State_Split_Finish  = "StateSplitFinish"
	State_Split_Failed  = "StateSplitFailed"

	Event_Merge_Prepare = "EventMergePrepare"
	Event_Merge_Failed  = "EventMergeFailed"
	Event_Merge_Success = "EventMergeSuccess"
	State_Merge_Begin   = "StateMergeBegin"
	State_Merge_Waiting = "StateMergeWaiting"
	State_Merge_Finish  = "StateMergeFinish"
	State_Merge_Failed  = "StateMergeFailed"
)

var (
	createEvents = fsm.Events{
		{Name: Event_Create_Prepare, Src: []string{State_Create_Begin}, Dst: State_Create_Waiting},
		{Name: Event_Create_Success, Src: []string{State_Create_Waiting}, Dst: State_Create_Finish},
		{Name: Event_Create_Failed, Src: []string{State_Create_Waiting}, Dst: State_Create_Failed},
	}
	createCallbacks = fsm.Callbacks{
		Event_Create_Prepare: func(event *fsm.Event) {

		},
		Event_Create_Failed: func(event *fsm.Event) {

		},
		Event_Create_Success: func(event *fsm.Event) {

		},
	}

	deleteEvents = fsm.Events{
		{Name: Event_Delete_Prepare, Src: []string{State_Delete_Begin}, Dst: State_Delete_Waiting},
		{Name: Event_Delete_Success, Src: []string{State_Delete_Waiting}, Dst: State_Delete_Finish},
		{Name: Event_Delete_Failed, Src: []string{State_Delete_Waiting}, Dst: State_Delete_Failed},
	}
	deleteCallbacks = fsm.Callbacks{
		Event_Delete_Prepare: func(event *fsm.Event) {

		},
		Event_Delete_Failed: func(event *fsm.Event) {

		},
		Event_Delete_Success: func(event *fsm.Event) {

		},
	}

	transferLeaderEvents = fsm.Events{
		{Name: Event_TransferLeader_Prepare, Src: []string{State_TransferLeader_Begin}, Dst: State_TransferLeader_Waiting},
		{Name: Event_TransferLeader_Success, Src: []string{State_TransferLeader_Waiting}, Dst: State_TransferLeader_Finish},
		{Name: Event_TransferLeader_Failed, Src: []string{State_TransferLeader_Waiting}, Dst: State_TransferLeader_Failed},
	}
	transferLeaderCallbacks = fsm.Callbacks{
		Event_TransferLeader_Prepare: func(event *fsm.Event) {

		},
		Event_TransferLeader_Failed: func(event *fsm.Event) {

		},
		Event_TransferLeader_Success: func(event *fsm.Event) {

		},
	}

	migrateEvents = fsm.Events{
		{Name: Event_Migrate_Prepare, Src: []string{State_Migrate_Begin}, Dst: State_Migrate_Waiting},
		{Name: Event_Migrate_Success, Src: []string{State_Migrate_Waiting}, Dst: State_Migrate_Finish},
		{Name: Event_Migrate_Failed, Src: []string{State_Migrate_Waiting}, Dst: State_Migrate_Failed},
	}
	migrateCallbacks = fsm.Callbacks{
		Event_Migrate_Prepare: func(event *fsm.Event) {

		},
		Event_Migrate_Failed: func(event *fsm.Event) {

		},
		Event_Migrate_Success: func(event *fsm.Event) {

		},
	}

	splitEvents = fsm.Events{
		{Name: Event_Split_Prepare, Src: []string{State_Split_Begin}, Dst: State_Split_Waiting},
		{Name: Event_Split_Success, Src: []string{State_Split_Waiting}, Dst: State_Split_Finish},
		{Name: Event_Split_Failed, Src: []string{State_Split_Waiting}, Dst: State_Split_Failed},
	}
	splitCallbacks = fsm.Callbacks{
		Event_TransferLeader_Prepare: func(event *fsm.Event) {

		},
		Event_TransferLeader_Failed: func(event *fsm.Event) {

		},
		Event_TransferLeader_Success: func(event *fsm.Event) {

		},
	}

	mergeEvents = fsm.Events{
		{Name: Event_Merge_Prepare, Src: []string{State_Merge_Begin}, Dst: State_Merge_Waiting},
		{Name: Event_Merge_Success, Src: []string{State_Merge_Waiting}, Dst: State_Merge_Finish},
		{Name: Event_Merge_Failed, Src: []string{State_Merge_Waiting}, Dst: State_Merge_Failed},
	}
	mergeCallbacks = fsm.Callbacks{
		Event_Merge_Prepare: func(event *fsm.Event) {

		},
		Event_Merge_Failed: func(event *fsm.Event) {

		},
		Event_Merge_Success: func(event *fsm.Event) {

		},
	}
)

func NewShardOperationFSM(operationType ShardOperationType) *fsm.FSM {
	switch operationType {
	case ShardOperationType_Create:
		createOperationFsm := fsm.NewFSM(
			State_Create_Begin,
			createEvents,
			createCallbacks,
		)
		return createOperationFsm
	case ShardOperationType_Delete:
		deleteOperationFsm := fsm.NewFSM(
			State_Delete_Begin,
			deleteEvents,
			deleteCallbacks)
		return deleteOperationFsm
	case ShardOperationType_TransferLeader:
		return nil
	case ShardOperationType_Migrate:
		return nil
	case ShardOperationType_Split:
		return nil
	case ShardOperationType_Merge:
		return nil
	}

	return nil
}
