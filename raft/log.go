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

	"github.com/pingcap-incubator/tinykv/log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	// * 已知在多数节点上的稳定存储中的最高日志位置。
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// * 已经应用到状态机的最大 log 位置
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	// * 已经被持久化存储的最大 log 位置。
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any. 传入的不稳定快照（如果有）。
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	tmp_Rlog := &RaftLog{
		storage:         storage,
		committed:       0,
		applied:         0,
		stabled:         0,
		entries:         []pb.Entry{},
		pendingSnapshot: nil,
	} // * 初始日志是空的
	// 现在添加一些初始条目
	// 获取初始条目索引
	firstIdx, err := storage.FirstIndex()
	if err != nil {
		log.Fatal(err)
		return nil
	}
	// 获取末尾条目索引
	lastIdx, err := storage.LastIndex()
	if err != nil {
		log.Fatal(err)
		return nil
	}
	// 获取这个范围内的条目，即 storage 中全部的
	initEntries, err := storage.Entries(firstIdx, lastIdx+1)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	tmp_Rlog.entries = append(tmp_Rlog.entries, initEntries...)
	tmp_Rlog.stabled = lastIdx // 存储中 读到的 最后一个有效的索引 就是 stable 的。
	return tmp_Rlog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return nil
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		if l.pendingSnapshot != nil {
			return l.pendingSnapshot.Metadata.Index
		}
		return 0
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
// 此处的索引 i 是 storage 中的 索引
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i == 0 { // TODO 0 索引的意思
		return 1, nil // TODO Term 1 的含义
	}
	if len(l.entries) == 0 { // 日志没有条目，检查快照。
		// 检查临时快照
		if l.pendingSnapshot != nil && i == l.pendingSnapshot.Metadata.Index {
			return l.pendingSnapshot.Metadata.Term, nil
		}
		// 检查存储中的永久快照
		snapshot, err := l.storage.Snapshot()
		if err != ErrSnapshotTemporarilyUnavailable && i == snapshot.Metadata.Index {
			return l.pendingSnapshot.Metadata.Term, nil
		}
		return 1, errors.New("entry id empty")
	}
	// 正常返回
	return l.entries[i-l.entries[0].Index].Term, nil
}
