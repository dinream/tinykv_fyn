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
	"fmt"
	"math/rand"
	"strconv"

	"github.com/pingcap-incubator/tinykv/log"
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

	delayElectionTimeout int
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
	// *从存储中恢复一些信息
	hardState, confState, err := c.Storage.InitialState()
	if c.peers == nil || len(c.peers) == 0 {
		c.peers = confState.Nodes
	}
	if err != nil {
		panic(err.Error())
	}
	var tmp_raft Raft
	tmp_raft.id = c.ID
	tmp_raft.Term = hardState.Term // 从最新的 Term 开始
	tmp_raft.State = StateFollower // 每个 Raft 的初始状态都为 follower
	tmp_raft.Vote = hardState.Vote // 初始 Vote 为
	tmp_raft.Lead = 0              // 数据库中不知道 lead ，初始化为 0
	tmp_raft.RaftLog = newLog(c.Storage)
	tmp_raft.electionTimeout = c.ElectionTick
	tmp_raft.heartbeatTimeout = c.HeartbeatTick
	tmp_raft.electionElapsed = 0
	tmp_raft.heartbeatElapsed = 0
	tmp_raft.leadTransferee = 0
	tmp_raft.PendingConfIndex = 0
	tmp_raft.votes = make(map[uint64]bool)
	// 初始化 map
	tmp_raft.Prs = make(map[uint64]*Progress)

	lastIndex, _ := c.Storage.LastIndex() // 用 偏移算出来的 可能比 tmp_raft.RaftLog.LastIndex() 准确一些
	for _, id := range c.peers {
		tmp_raft.votes[id] = false
		tmp_raft.Prs[id] = &Progress{
			Match: hardState.Commit, // 注意  match 是 共识 的，而不是 已经 应用的
			Next:  lastIndex + 1,
		}
	}
	return &tmp_raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
// * 消息发送才 返回 true
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if _, ok := r.Prs[to]; !ok {
		return false
	}
	// TODO 进一步验证
	// 需要的条目索引小于存储范围
	if r.Prs[to].Next < r.RaftLog.entries[0].Index {
		// 快照不一致，导致 RaftLog 超出范围，
		snapshot, err := r.RaftLog.storage.Snapshot()
		if err != nil {
			log.Infof("snapshot not ready")
			return false
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType:  pb.MessageType_MsgSnapshot,
			To:       to,
			From:     r.id,
			Term:     r.Term,
			Commit:   r.RaftLog.committed,
			Snapshot: &snapshot,
		})
		return true
	}

	// TODO 需要的条目索引大于存储范围  进一步验证
	if r.Prs[to].Next > r.RaftLog.LastIndex() {
		return false
	}
	// TODO 为什么是上一个条目的日志和索引
	arrayIndex := r.Prs[to].Next - r.RaftLog.entries[0].Index
	var prevLogIndex, prevLogTerm uint64
	if arrayIndex == 0 { // 期待 第 0 号元素
		prevLogIndex = 0
		prevLogTerm = 0
	} else {
		prevLogIndex = uint64(int(r.RaftLog.entries[arrayIndex-1].Index))
		prevLogTerm, _ = r.RaftLog.Term(prevLogIndex)
	}
	appendEntries := []*pb.Entry{}
	// 发送 从数组下标开始到末尾的所有 条目
	for int(arrayIndex) < len(r.RaftLog.entries) {
		appendEntries = append(appendEntries, &r.RaftLog.entries[arrayIndex])
		arrayIndex++
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: appendEntries,
		Commit:  r.RaftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
// TODO Leader 给 follower ？
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		From:    r.id,
		To:      to,
		MsgType: pb.MessageType_MsgHeartbeat,
		Term:    r.Term,
		Commit:  min(r.RaftLog.committed, r.Prs[to].Match), // 确保了心跳消息中的 Commit 字段不会超过已经在目标节点上提交的日志索引，避免了不必要的信息重复传输
	})
}

func (r *Raft) sendRequestVote() {
	voteReq := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      0,
		From:    r.id,
		Term:    r.Term,
	}
	// get last entry's term and index
	if len(r.RaftLog.entries) != 0 {
		voteReq.Index = r.RaftLog.LastIndex()              // TODO 这里有什么用
		voteReq.LogTerm, _ = r.RaftLog.Term(voteReq.Index) // TODO 这里有什么用
	}
	// TODO 如果 没有 log entries 就不做了吗
	for peer, _ := range r.Prs {
		if peer == r.id {
			continue
		}
		voteReq.To = peer
		r.msgs = append(r.msgs, voteReq)
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionElapsed++
	r.heartbeatElapsed++
	// 心跳超时
	if r.heartbeatElapsed >= r.heartbeatTimeout { // 归零在消息处理时进行
		if r.State == StateLeader {
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
			if err != nil {
				return
			}
		}
	}
	// 选举超时
	if r.electionElapsed >= r.electionTimeout { // 归零在消息处理时进行
		if r.State == StateLeader {
			return
		}
		err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		if err != nil {
			return
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// if len(r.votes) == 1 { // 如果就只有一个 Raft 当场变成 leader  // 不需要特殊处理，走流程即可
	// 	r.becomeLeader()
	// }
	r.State = StateCandidate
	r.Term++
	// log.Infof("Enter in becomeCandate(),term:%d\n", r.Term)
	r.msgs = []pb.Message{}
	for id := range r.votes {
		if id == r.id {
			continue
		}
		r.votes[id] = false
	}
	r.Vote = r.id        // 当场给自己投票
	r.votes[r.id] = true // 自己给自己投票
	// r.electionElapsed = 0
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	r.Lead = r.id
	r.State = StateLeader
	// for id := range r.votes {
	// 	r.votes[id] = false			// 不需要，只有 候选者 需要用这个进行统计，但是候选者自己会清空
	// }
	noopMsg := pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		To:      r.id,
		From:    r.id,
		Term:    r.Term,
		Entries: []*pb.Entry{&pb.Entry{
			EntryType: pb.EntryType_EntryNormal,
			Term:      r.Term,
			Index:     r.RaftLog.LastIndex() + 1,
			Data:      nil,
		}},
	}
	if err := r.Step(noopMsg); err != nil {
		log.Fatal(err)
	}
}

// Step the entrance of handle message, see `MessageType` 处理消息的入口
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// 判断当前的 msg 目标是不是 当前的 Raft

	// if m.To != r.id {
	// 	return errors.New("dest id is not cur id")
	// }

	// 判断当前的 msg 来源是否是 有效的 raft
	if _, ok := r.Prs[m.From]; !ok && m.From != 0 { // TODO
		if m.MsgType == pb.MessageType_MsgHeartbeat {
			r.Vote = m.From
			r.becomeFollower(m.Term, m.From)
			r.Prs = make(map[uint64]*Progress)
			r.addNode(r.id)
			r.addNode(m.From)
		} else {
			return errors.New("resc id is not available")
		}
	}

	// TODO 什么时候进行触发消息选举开始发送消息
	// TODO 是不是每一次发送消息之前都要 心跳
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.handleHup()
		case pb.MessageType_MsgAppend:
			// log.Infof("weishenmeaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:%d \n", m.From)
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgTimeoutNow:
			r.handleTimeoutNow()
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.handleHup()
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote: // 候选者 收到其他后选者的期待投票
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse: //候选者收到其他 follower  对自己的投票回应
			r.handleRequestVoteResponse(m)

		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgTimeoutNow:
			r.handleTimeoutNow()
		}
	case StateLeader: // 1. leader 需要 维护 peers
		switch m.MsgType {
		case pb.MessageType_MsgHup: // Leader 不会自己发起一个新的选举
			r.handleHup()
		case pb.MessageType_MsgBeat:
			r.handleBeat(m)
		case pb.MessageType_MsgPropose:
			r.handlePropose(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendEntriesResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		case pb.MessageType_MsgTimeoutNow:
			r.handleTimeoutNow()
		}
	}
	return nil
}

// TODO 这里的 时间变化
func (r *Raft) handleHup() {
	r.electionElapsed = -1 * (rand.Int() % r.electionTimeout)
	r.electionElapsed -= r.delayElectionTimeout
	r.delayElectionTimeout = 0
	if r.State == StateLeader {
		return
	}
	r.becomeCandidate()
	r.sendRequestVote()
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
}

func (r *Raft) handleBeat(m pb.Message) {
	// Your Code Here (2A).
	// 当 Tick() -> MsgBeat 时， Leader 需要 广播一次心跳。
	for follow, _ := range r.Prs {
		if follow == r.id {
			continue
		}
		r.sendHeartbeat(follow)
	}
	r.heartbeatElapsed = 0
}

func (r *Raft) handlePropose(m pb.Message) {
	if r.leadTransferee != 0 {
		if r.Lead != r.leadTransferee {
			log.Infof("peerid=%d, TransferLeader is in progress, proposal is hold on, transferee=%d", r.id, r.leadTransferee)
			return
		} else {
			r.leadTransferee = 0
		}
	}
	lastIndex := r.RaftLog.LastIndex()
	if lastIndex == 0 {
		lastIndex = r.RaftLog.committed
		for peer, _ := range r.Prs {
			if peer != r.id {
				r.Prs[peer].Match = r.RaftLog.committed
				r.Prs[peer].Next = r.RaftLog.committed + 1
			}
		}
	}
	for i, entry := range m.Entries {
		entry.Index = lastIndex + uint64(i) + 1
		entry.Term = r.Term
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	//r.Prs[r.id].Match = lastIndex
	//r.Prs[r.id].Next = lastIndex + 1
	log.Infof("Handle Proposal %s, %s", r.String(), m.String())
	for peer, _ := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	rsp := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    m.To,
		Term:    m.Term,
	}
	if m.Term < r.Term {
		rsp.Term = r.Term
		r.msgs = append(r.msgs, rsp)
		return
	}
	r.Lead = m.From
	r.becomeFollower(m.Term, m.From)
	r.heartbeatElapsed = 0
	r.electionElapsed = -1 * (rand.Int() % r.electionTimeout)
	r.Vote = 0
	if m.Commit >= r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
		r.RaftLog.committed = max(r.RaftLog.committed, r.RaftLog.applied)
	}
	r.msgs = append(r.msgs, rsp)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// Your Code Here (2A).
	if r.Term < m.Term {
		// becomeFollow
		r.becomeFollower(m.Term, m.From)
		r.electionElapsed = -1 * (rand.Int() % r.electionTimeout)
		r.Vote = 0
		return
	}
	if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	// get last entry's term and index————————————————————————
	var lastEntryIndex, lastEntryTerm uint64
	lastEntryIndex = 0
	lastEntryTerm = 0
	if len(r.RaftLog.entries) != 0 {
		lastEntryIndex = r.RaftLog.LastIndex()
		lastEntryTerm, _ = r.RaftLog.Term(lastEntryIndex)
	}
	requestVoteResponse := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    m.To,
		Term:    m.Term,
		Reject:  true,
	} // 定义一个初始的回应
	// log.Error(r.id, r.Vote, lastEntryIndex, m.Index, lastEntryTerm, m.LogTerm, (r.Vote == 0 || r.Vote == m.From) && (m.LogTerm > lastEntryTerm ||
	//	(m.LogTerm >= lastEntryTerm && m.Index >= lastEntryIndex)), r.Term, m.Term)
	if r.Term > m.Term {
		requestVoteResponse.Reject = true
		requestVoteResponse.Term = r.Term
	} else if r.Term == m.Term {
		if (r.Vote == r.id) && (m.LogTerm > lastEntryTerm ||
			(m.LogTerm >= lastEntryTerm && m.Index > lastEntryIndex)) {
			r.delayElectionTimeout = r.electionTimeout / 2
		}
		if (r.Vote == 0 || r.Vote == m.From) && (m.LogTerm > lastEntryTerm ||
			(m.LogTerm >= lastEntryTerm && m.Index >= lastEntryIndex)) {
			requestVoteResponse.Reject = false
			requestVoteResponse.Term = r.Term
			r.Vote = m.From
			if m.From != r.id {
				r.State = StateFollower
				r.Lead = 0
				// r.electionElapsed = 0
				r.electionElapsed = -1 * (rand.Int() % r.electionTimeout)
			}
		}
	} else {
		if m.LogTerm > lastEntryTerm ||
			(m.LogTerm == lastEntryTerm && m.Index >= lastEntryIndex) {
			requestVoteResponse.Reject = false
			r.Vote = m.From
		} // 即使 消息的 Term 比 raft 的大，也可能投拒绝票
		r.Term = m.Term
		requestVoteResponse.Term = m.Term
		r.State = StateFollower
		r.Lead = 0
		// r.electionElapsed = 0
		r.electionElapsed = -1 * (rand.Int() % r.electionTimeout)
	}
	if lastEntryTerm > m.LogTerm ||
		(lastEntryTerm == m.LogTerm && lastEntryIndex > m.Index) {
		requestVoteResponse.LogTerm = lastEntryTerm
		requestVoteResponse.Index = lastEntryIndex
	}
	if requestVoteResponse.Reject == false {
		r.Lead = r.Vote
	}
	r.msgs = append(r.msgs, requestVoteResponse)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	// Your Code Here (2A).
	if r.Term < m.Term {
		r.State = StateFollower
		r.Lead = 0
		return
	}
	if m.Reject {
		r.votes[m.From] = false
		// r.votes[m.From+DenyVoteIDOffset] = true
	} else {
		r.votes[m.From] = true
	}
	voteCnt := 0
	// denyCnt := 0
	totalCnt := len(r.Prs)
	for id, _ := range r.Prs {
		if r.votes[id] {
			voteCnt++
		}
		// else if r.votes[id+DenyVoteIDOffset] {
		// 	denyCnt++
		// }
	}
	if voteCnt*2 > totalCnt && r.State != StateLeader {
		r.becomeLeader()
	}
	// if denyCnt*2 > totalCnt {
	// 	r.State = StateFollower
	// 	r.Vote = 0
	// }
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	log.Infof("lint: %d\n", len(r.RaftLog.entries))
	if r.Term <= m.Term {
		if r.id != m.From {
			r.becomeFollower(m.Term, m.From)
			r.heartbeatElapsed = 0
			r.electionElapsed = -1 * (rand.Int() % r.electionTimeout)
			if r.Vote != m.From {
				r.Vote = 0
			}
		}
	} else {
		log.Infof("weishenme \n")
		return
	}
	r.Lead = m.From
	rsp := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    m.To,
		Term:    r.Term,
		Reject:  true,
	}
	offset := 0
	// log.Infof("%d %d %d %d %d", len(r.RaftLog.entries), r.RaftLog.stabled, r.RaftLog.applied, r.RaftLog.committed, m.Index)
	/*if len(r.RaftLog.entries) == 0 && r.RaftLog.applied > 0 &&  {
		//
		log.Infof("peerid=%d is newly added node", r.id)
		rsp.Reject = false
		rsp.Index = 0
		r.msgs = append(r.msgs, rsp)
		return
	}*/
	if m.Index != 0 || m.LogTerm != 0 {
		localPrevLogTerm, err := r.RaftLog.Term(m.Index)
		if err != nil || (m.LogTerm != localPrevLogTerm) {
			r.msgs = append(r.msgs, rsp)
			return
		}
		prevLogArrayIndex := int(m.Index - r.RaftLog.entries[0].Index)
		offset = prevLogArrayIndex + 1
	}
	idx := 0
	// TODO 搞清楚！
	oldEntries := make([]pb.Entry, len(r.RaftLog.entries))
	copy(oldEntries, r.RaftLog.entries)
	isConfilict := false
	for offset+idx < len(r.RaftLog.entries) && idx < len(m.Entries) {
		if r.RaftLog.entries[offset+idx].Index != (*m.Entries[idx]).Index ||
			r.RaftLog.entries[offset+idx].Term != (*m.Entries[idx]).Term ||
			string(r.RaftLog.entries[offset+idx].Data) != string((*m.Entries[idx]).Data) {
			isConfilict = true
		}
		r.RaftLog.entries[offset+idx] = *m.Entries[idx]
		idx++
	}
	for idx < len(m.Entries) {
		r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[idx])
		idx++
	}
	for offset+idx < len(r.RaftLog.entries) {
		if isConfilict {
			r.RaftLog.entries = r.RaftLog.entries[:offset+idx]
			break
		}
		if offset+idx >= 1 && (r.RaftLog.entries[offset+idx].Term < r.RaftLog.entries[offset+idx-1].Term ||
			r.RaftLog.entries[offset+idx].Index <= r.RaftLog.entries[offset+idx-1].Index) {
			r.RaftLog.entries = r.RaftLog.entries[:offset+idx]
			break
		}
		idx++
	}
	idx = 0

	debugStr := strconv.Itoa(int(r.id)) + " oldEntry: "
	for i := 0; i < len(oldEntries); i++ {
		debugStr += strconv.FormatUint(oldEntries[i].Index, 10) + " "
	}
	//log.Infof(debugStr)

	debugStr = strconv.Itoa(int(r.id)) + " newEntry: "
	for i := 0; i < len(r.RaftLog.entries); i++ {
		debugStr += strconv.FormatUint(r.RaftLog.entries[i].Index, 10) + " "
	}
	//log.Infof(debugStr)

	for idx < len(oldEntries) && idx < len(r.RaftLog.entries) {
		if oldEntries[idx].Index != r.RaftLog.entries[idx].Index ||
			oldEntries[idx].Term != r.RaftLog.entries[idx].Term {
			//log.Infof("peerid=%d set stabled from %d to %d", r.id, r.RaftLog.stabled, min(r.RaftLog.stabled, oldEntries[idx].Index-1))
			tmpStabled := r.RaftLog.stabled
			r.RaftLog.stabled = min(r.RaftLog.stabled, oldEntries[idx].Index-1)
			if r.RaftLog.stabled+1 < r.RaftLog.entries[0].Index {
				r.RaftLog.stabled = tmpStabled
				rsp.Reject = true
				r.RaftLog.entries = make([]pb.Entry, len(oldEntries))
				copy(r.RaftLog.entries, oldEntries)
				r.msgs = append(r.msgs, rsp)
				return
			}
			//}
			break
		}
		idx++
	}
	rsp.Reject = false
	rsp.Index = r.RaftLog.LastIndex()
	r.msgs = append(r.msgs, rsp)
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
		r.RaftLog.committed = min(r.RaftLog.committed, m.Index+uint64(len(m.Entries)))
		r.RaftLog.committed = max(r.RaftLog.committed, r.RaftLog.applied)
	}
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	// Your Code Here (2A).
	if r.Term < m.Term {
		// becomeFollow
		r.State = StateFollower
		r.Vote = 0
		return
	}
	if m.Reject {
		r.Prs[m.From].Next--
		r.sendAppend(m.From)
		return
	} else {
		r.Prs[m.From].Next = m.Index + 1
		r.Prs[m.From].Match = m.Index
	}
	N := r.RaftLog.LastIndex()
	// log.Error(N, r.RaftLog.entries)
	for N >= r.RaftLog.committed+1 {
		numMatched := 0
		for _, progres := range r.Prs {
			if progres.Match >= N {
				numMatched++
			}
		}
		term, err := r.RaftLog.Term(N)
		if err != nil {
			return
		}
		if numMatched*2 > len(r.Prs) && r.Term == term {
			if r.RaftLog.committed != N {
				r.RaftLog.committed = N
				for peer, _ := range r.Prs {
					if peer == r.id {
						continue
					}
					/*r.msgs = append(r.msgs, pb.Message{
						MsgType: pb.MessageType_MsgAppend,
						To:      peer,
						From:    r.id,
						Term:    r.Term,
						//Commit:  r.RaftLog.committed,
						Commit: min(r.RaftLog.committed, r.Prs[peer].Match),
					})*/
					r.sendAppend(peer)
				}
			}
			break
		}
		N--
	}
	//log.Errorf("%d %d %d", r.leadTransferee, r.Prs[m.From].Match, r.RaftLog.LastIndex())
	if r.leadTransferee != 0 && r.Prs[m.From].Match == r.RaftLog.LastIndex() {
		log.Infof("peerid=%d, sendAppend to leaderTransferee=%d", r.id, r.leadTransferee)
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgTimeoutNow,
			To:      r.leadTransferee,
			From:    r.id,
			Term:    r.Term,
		})
		// r.leadTransferee = 0
	}
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

func (r *Raft) handleTimeoutNow() {
	log.Infof("peerid=%d will become leader immediately for LeaderTransfer", r.id)
	if r.State == StateLeader {
		return
	}
	r.becomeCandidate()
	r.sendRequestVote()
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
}

func (r *Raft) String() string {
	return fmt.Sprintf("RaftState:{peerid: %d Term: %d Vote: %d commited: %d State: %s}",
		r.id, r.Term, r.Vote, r.RaftLog.committed, r.State.String())
}
