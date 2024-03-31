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
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases, 当提案者因为某些情况被忽视时,会返回 ErrProposalDropped.
// so that the proposer can be notified and fail fast. 以便于提案者可以收到通知并快速失败
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only  // 使用旧版本的 configure 可能会引起 panic
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	/**Applied 是最后应用的索引。 仅应在重新启动 raft 时设置。 raft 不会将小于或等于 Applied 的条目返回到应用程序。 如果重新启动时未设置 Applied，则 raft 可能会返回之前应用的条目。 这是一个非常依赖于应用程序的配置。*/
	// TODO 什么东西
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
// 这个是 Leader 眼中 Follower 的 Progress ，Leader 维护所有 follower 的进度，并根据其进度向 follower 发送条目。
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	// TODO 什么东西
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	// TODO leader 的心跳是向谁的
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate. 候选则者或者 leader 的任期时间
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.			// follower 的 收到 Leader 命令的时间间隔 ，或者 Leader 和 Candidate 当的时间
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.  // 当其不为零时，这是 leader  的发送目标 id
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer) // 领导者转移
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending			// 类值更改的日志索引
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	// TODO 干嘛用的，更新配置，跟着 leader 更新
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	var tmp_raft Raft
	tmp_raft.id = c.ID
	tmp_raft.Term = 0              // 初始 Term 号 为 0
	tmp_raft.State = StateFollower // 每个 Raft 的初始状态都为 follower
	tmp_raft.Lead = 0              // 初始 Lead 为 follower
	tmp_raft.electionTimeout = c.ElectionTick
	tmp_raft.heartbeatTimeout = c.HeartbeatTick
	tmp_raft.electionElapsed = 0
	tmp_raft.heartbeatElapsed = 0
	tmp_raft.leadTransferee = 0
	// 初始化 map
	tmp_raft.Prs = make(map[uint64]*Progress)
	for i := 0; i < len(c.peers); i++ {
		tmp_raft.Prs[c.peers[i]] = &Progress{
			0, 0,
		}
	}
	return &tmp_raft
	// return nil
	// TODO 目前 config 中的 peers id 和 storage 还没有用到
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// 发送一个 增加  消息
	var msg pb.Message

	var entry pb.Entry
	entry.EntryType = 0 // 普通的日志添加
	entry.Term = r.Term
	entry.Index = r.RaftLog.LastIndex()
	entry.Data = r.RaftLog.pendingSnapshot.GetData() // 数据
	entry.XXX_unrecognized = r.RaftLog.pendingSnapshot.XXX_unrecognized
	entry.XXX_sizecache = r.RaftLog.pendingSnapshot.XXX_sizecache
	entry.XXX_NoUnkeyedLiteral = r.RaftLog.pendingSnapshot.XXX_NoUnkeyedLiteral

	msg.From = r.id
	msg.To = r.leadTransferee
	msg.Term = r.Term
	msg.MsgType = pb.MessageType_MsgAppend

	// TODO 什么是 MeteData

	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	var msg pb.Message
	msg.MsgType = pb.MessageType_MsgBeat
	// Your Code Here (2A).
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionElapsed++
	r.heartbeatElapsed++
}

// TODO raft log？
// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.Vote = 0
	r.State = StateFollower
	for id := range r.votes {
		if id == r.id {
			continue // 自己不给自己发消息
		}
		r.votes[id] = false
		// r.msgs = append(r.msgs, pb.Message{
		// 	From:    r.id,
		// 	To:      id,
		// 	MsgType: pb.MessageType_MsgRequestVote,
		// })
	}
	r.Lead = lead
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if len(r.votes) == 1 { // 如果就只有一个 Raft 当场变成 leader
		r.becomeLeader()
	}
	r.Vote = r.id // 当场给自己投票
	r.State = StateCandidate
	var tmp_msg []pb.Message
	for id := range r.votes {
		if id == r.id {
			continue // 自己不给自己发消息
		}
		r.votes[id] = false
		tmp_msg = append(tmp_msg, pb.Message{
			From:    r.id,
			To:      id,
			MsgType: pb.MessageType_MsgRequestVote,
		})
	}
	r.msgs = append(r.msgs, tmp_msg...)
	r.votes[r.id] = true  // 自己给自己投票
	r.electionElapsed = 0 // * 重新开始选举
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	r.Vote = 0
	r.State = StateLeader
	for id := range r.votes {
		if id == r.id {
			continue // 自己不给自己发消息
		}
		r.votes[id] = false
		r.msgs = append(r.msgs, pb.Message{
			From:    r.id,
			To:      id,
			MsgType: pb.MessageType_MsgAppend,
		})
	}
	r.heartbeatElapsed = 0
}

// Step the entrance of handle message, see `MessageType` 处理消息的入口
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// 判断当前的 msg 目标是不是当前的 Raft
	if m.To != r.id {
		return errors.New("dest id is not cur id")
	}
	// TODO 什么时候进行触发消息选举开始发送消息
	// TODO 是不是每一次发送消息之前都要 心跳
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			// r.Term++ 发送这个消息之前就已经 增加了。
			if r.Term <= m.Term {
				if r.Term < m.Term {
					r.Term = m.Term // 一般不会出现这种情况
				}
				r.becomeCandidate()
			}
		case pb.MessageType_MsgBeat:
			// follower 收到  leader 的心跳 ?
			if r.Lead == m.From {
				r.electionElapsed = 0
			}
		case pb.MessageType_MsgPropose:
		case pb.MessageType_MsgAppend:
		case pb.MessageType_MsgAppendResponse:
		case pb.MessageType_MsgRequestVote:
			if m.Term > r.Term {
				r.becomeCandidate()
			}
		case pb.MessageType_MsgRequestVoteResponse:
		case pb.MessageType_MsgSnapshot:
		case pb.MessageType_MsgHeartbeat:
			r.msgs = append(r.msgs, pb.Message{
				From:    r.id,
				To:      m.From,
				MsgType: pb.MessageType_MsgHeartbeatResponse,
			})
		case pb.MessageType_MsgHeartbeatResponse:
			// 只能说明 leader 正常工作中。
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
		case pb.MessageType_MsgBeat:
		case pb.MessageType_MsgPropose:
		case pb.MessageType_MsgAppend:
		case pb.MessageType_MsgAppendResponse:
		case pb.MessageType_MsgRequestVote: // 候选者 收到其他后选者的期待投票
			// 其他候选者 的 term 更高
			if m.Term > r.Term {
				// r.becomeFollower()
				// TODO 发送选举同意消息
			}
		// 否则 只是投自己
		case pb.MessageType_MsgRequestVoteResponse: //候选者收到其他 follower  对自己的投票回应
		// 验证是否是投自己的票。
		// 如果是投满票了，直接变成 leader
		case pb.MessageType_MsgSnapshot:
		case pb.MessageType_MsgHeartbeat:
			// 心跳还是正常的
		case pb.MessageType_MsgHeartbeatResponse:
			// 心跳的回应也是正常的
		}
	case StateLeader: // 1. leader 需要 维护 peers
		switch m.MsgType {
		// case pb.MessageType_MsgHup:
		case pb.MessageType_MsgBeat:
			// r.Prs[m.From].
		case pb.MessageType_MsgPropose:
			if m.Term > r.Term {
				// r.becomeFollower()
				// TODO 发送选举同意消息
			}
		case pb.MessageType_MsgAppendResponse:
			// TODO 统计已经回复的 数量，超过一半返回写入消息
		case pb.MessageType_MsgRequestVote:
			if m.Term > r.Term {
				r.becomeFollower(m.Term, m.From)
				// TODO 发送选举同意消息 改变选举目标，删除 lead
			}
		// case pb.MessageType_MsgRequestVoteResponse:  // leader 不可能在发送一个选举
		case pb.MessageType_MsgSnapshot:
		case pb.MessageType_MsgHeartbeat:
		case pb.MessageType_MsgHeartbeatResponse:
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
