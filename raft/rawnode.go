// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

	//"github.com/pingcap/log"

	"go.uber.org/atomic"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

func (ss *SoftState) compareSoftState(state *SoftState) bool {
	if ss.Lead != state.Lead || ss.RaftState != state.RaftState {
		return false
	}
	return true
}

// Ready encapsulates the entries and messages that are ready to read, Ready封装了准备读取的条目和消息，
// be saved to stable storage, committed or sent to other peers.	保存到稳定存储、提交或发送给其他对等点。
// All fields in Ready are read-only.	所有字段都是只读的。
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

// RawNode is a wrapper of Raft. 原始节点，对 Raft 的封装
type RawNode struct {
	Raft *Raft
	// Your Data Here (2A).
	softState *SoftState
	hardState pb.HardState
	snapshot  *pb.Snapshot // TODO 可能是临时（未永久存储的）快照？
	readble   atomic.Bool
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	// Your Code Here (2A).
	raft := newRaft(config)
	ret := &RawNode{
		Raft:      raft,
		softState: raft.softState(),
		hardState: raft.hardState(),
	}

	// TODO 快照初始为空？
	ret.snapshot = &pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{},
	}
	return ret, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal() // 结构化为字节数组
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
// 返回处理后的 nodes 数组
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg // 本地的消息会在 Raft 层面自动调用 Step 推进
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) { // 注意这里的判断条件：要么对此节点有记录，要么这个消息不是回应类型的消息。
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready returns the current point-in-time state of this RawNode. Ready 返回此 RawNode 的当前时间点状态。
// Ready 获取当前节点相对于永久存储的进度
func (rn *RawNode) Ready() Ready {
	// Your Code Here (2A).
	ready := Ready{
		SoftState:        nil,
		HardState:        pb.HardState{},
		Entries:          rn.Raft.RaftLog.unstableEntries(),
		Snapshot:         pb.Snapshot{},
		CommittedEntries: rn.Raft.RaftLog.nextEnts(),
		Messages:         rn.Raft.msgs,
	}

	curSoftState := rn.Raft.softState()
	if !(curSoftState.Lead == rn.softState.Lead &&
		curSoftState.RaftState == rn.softState.RaftState) {
		ready.SoftState = curSoftState
		rn.softState = curSoftState
	}

	curHardState := rn.Raft.hardState()
	if !isHardStateEqual(curHardState, rn.hardState) {
		ready.HardState = curHardState
		// rn.prevHardState = curHardState
	}

	if !IsEmptySnap(rn.Raft.RaftLog.pendingSnapshot) {
		ready.Snapshot = *rn.Raft.RaftLog.pendingSnapshot
		// rn.Raft.RaftLog.pendingSnapshot = nil
	}

	rn.Raft.msgs = nil
	return ready
}

// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	// Your Code Here (2A).
	// 比较软状态
	curSoftState := rn.Raft.softState()
	if !(curSoftState.Lead == rn.softState.Lead &&
		curSoftState.RaftState == rn.softState.RaftState) {
		return true
	}
	// 比较硬状态
	curhardState := rn.Raft.hardState()
	if !IsEmptyHardState(curhardState) &&
		!isHardStateEqual(curhardState, rn.hardState) {
		return true
	}

	// 有需要应用的条目 或者 有为发送的消息
	if len(rn.Raft.RaftLog.unstableEntries()) > 0 ||
		len(rn.Raft.msgs) > 0 || len(rn.Raft.RaftLog.nextEnts()) > 0 {
		return true
	}
	// 有快照需要应用
	if !IsEmptySnap(rn.Raft.RaftLog.pendingSnapshot) {
		return true
	}

	return false
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
func (rn *RawNode) Advance(rd Ready) {
	// Your Code Here (2A).
	// if rd.Snapshot.Metadata != nil {
	// 	log.Infof("peerid=%d advance snapshot", rn.Raft.id)
	// 	rn.Raft.RaftLog.pendingSnapshot = nil
	// 	return
	// }
	if len(rd.Entries) > 0 {
		// log.Infof("peerid=%d set stabled from %d to %d", rn.Raft.id, rn.Raft.RaftLog.stabled, rd.Entries[len(rd.Entries)-1].Index)
		rn.Raft.RaftLog.stabled = rd.Entries[len(rd.Entries)-1].Index
	}
	if len(rd.CommittedEntries) > 0 {
		rn.Raft.RaftLog.applied = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
	}
	if len(rn.Raft.msgs) != 0 {
		rn.Raft.msgs = rn.Raft.msgs[:0]
	}
	if !IsEmptyHardState(rd.HardState) {
		rn.hardState = rd.HardState
	}
	if !IsEmptySnap(rn.Raft.RaftLog.pendingSnapshot) {
		rn.Raft.RaftLog.pendingSnapshot = nil
	}
}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}
