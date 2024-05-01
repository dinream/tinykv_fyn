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
	"strconv"

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
	if firstIdx == 7 {
		log.Info("xixixixii\n")
	}
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
	hardState, _, _ := storage.InitialState()
	// 获取这个范围内的条目，即 storage 中全部的
	initEntries, err := storage.Entries(firstIdx, lastIdx+1)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	tmp_Rlog.entries = append(tmp_Rlog.entries, initEntries...)
	tmp_Rlog.stabled = lastIdx // 存储中 读到的 最后一个有效的索引 就是 stable 的。
	tmp_Rlog.committed = hardState.Commit
	tmp_Rlog.applied = firstIdx - 1
	return tmp_Rlog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	// 进行 日志条目的压缩,主要是紧凑 内存中的日志,即 l.entries : 不需要实现,没有看到调用
	store_index, _ := l.storage.FirstIndex()
	if len(l.entries) > 0 {
		firstIndex := l.entries[0].Index
		if store_index > firstIndex { // 更新 l.entries
			l.entries = l.entries[store_index-firstIndex:]
		}
	}
}

// allEntries return all the entries not compacted. 返回所有未压缩的目录
// note, exclude any dummy entries from the return value. 排除虚拟条目
// note, this is one of the test stub functions you need to implement. 	测试存根函数之一
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	ret := []pb.Entry{}
	for _, ent := range l.entries {
		// if ent.Data != nil { // 如果 这里的 ent 的 Data 是空的,说明 entry 无效
		// }
		ret = append(ret, ent)
	}
	return ret
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return []pb.Entry{}
	}
	if l.stabled == 0 {
		// 对于一个 stabled = 0 的 raft ，
		// 它可能没有通过 config 中 storage 来初始化，这种初始化在网络中也是可能出现的
		// 此时它的每一个 entry 都是 unstable
		return l.entries
		// log.Infof("stabled = %d", l.stabled)
	}
	if !(int(l.stabled-l.entries[0].Index)+1 >= 0 && int((l.stabled-l.entries[0].Index)+1) < len(l.entries)) {
		// 如果 l.stabled 不在 l.entries 的范围之内: 最小是:l.entries[0].Index 的前一个,最大是 l.entries 的最后一个 Index
		// (l.stabled-l.entries[0].Index)+1 表示,下一个 unstabled 的下标
		return []pb.Entry{}
	}

	return l.entries[(l.stabled-l.entries[0].Index)+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	if l.committed < l.applied {
		log.Infof("committed:%d < applied:%d \n", l.committed, l.applied)
		return nil
	}
	if !(int(l.applied-l.entries[0].Index+1) >= 0 && int(l.committed-l.entries[0].Index) < len(l.entries)) {
		return nil
	}

	return l.entries[l.applied-l.entries[0].Index+1 : l.committed-l.entries[0].Index+1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		if l.pendingSnapshot != nil {
			return l.pendingSnapshot.Metadata.Index
		}
		return 0 // TODO 这里是默认的 初始 index 我这里使用 0，是否要换成 5，不换时已经通过了 2b
	}
	return l.entries[len(l.entries)-1].Index
}
func (l *RaftLog) FirstIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		i, _ := l.storage.FirstIndex()
		return i - 1
	}
	return l.entries[0].Index
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
	if !(i >= l.entries[0].Index && i <= l.entries[len(l.entries)-1].Index) {
		log.Infof("hahahahaha:out of range: i: %d,,,,,,l.entries[0].Index: %d\n", i, l.entries[0].Index)
		return 1, errors.New("index" + strconv.Itoa(int(i)) + " out of range")
	}
	// 正常返回
	return l.entries[i-l.entries[0].Index].Term, nil
}

// 用于格式化输出。
func (l *RaftLog) String() string {
	entriesStr := "{"
	for _, entry := range l.entries {
		entriesStr += " [" + entry.String() + "] "
	}
	entriesStr += "}"
	return fmt.Sprintf("RaftLog:{commit: %d applied: %d stabled: %d entries:%s}", l.committed, l.applied, l.stabled, entriesStr)
}
