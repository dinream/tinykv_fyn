package raftstore

import (
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
)

// raftWorker is responsible for run raft commands and apply raft logs.
type raftWorker struct {
	// TODO peer 之间？
	pr *router // 管道通信的接口，节点状态 ——> 发送 peer + 发送 store

	// receiver of messages should sent to raft, including:
	// * raft command from `raftStorage`
	// * raft inner messages from other peers sent by network
	// TODO raft 之间
	raftCh chan message.Msg // 来自 raftStorage 的消息 和 通过网络发送的 Raft 内部消息
	ctx    *GlobalContext   // 全局环境

	closeCh <-chan struct{}
}

func newRaftWorker(ctx *GlobalContext, pm *router) *raftWorker {
	return &raftWorker{
		raftCh: pm.peerSender,
		ctx:    ctx,
		pr:     pm,
	}
}

// run runs raft commands.
// On each loop, raft commands are batched by channel buffer.
// After commands are handled, we collect apply messages by peers, make a applyBatch, send it to apply channel.
// run 运行 raft 命令。
// 在每个循环中，raft 命令由通道缓冲区进行批处理。
// 处理完命令后，我们收集对等点的应用消息，制作一个 applyBatch，将其发送到应用通道。
func (rw *raftWorker) run(closeCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	var msgs []message.Msg
	for {
		msgs = msgs[:0] // 清空之前的消息
		select {        // 利用 select 实现返回或者阻塞
		case <-closeCh:
			return
		case msg := <-rw.raftCh:
			msgs = append(msgs, msg)
		}
		// 将剩余的消息加入数组。
		pending := len(rw.raftCh)
		for i := 0; i < pending; i++ {
			msgs = append(msgs, <-rw.raftCh)
		}
		peerStateMap := make(map[uint64]*peerState)
		for _, msg := range msgs { // 处理每一个有效 peer 的信息
			peerState := rw.getPeerState(peerStateMap, msg.RegionID)
			if peerState == nil {
				continue
			}
			newPeerMsgHandler(peerState.peer, rw.ctx).HandleMsg(msg)
		}
		for _, peerState := range peerStateMap {
			newPeerMsgHandler(peerState.peer, rw.ctx).HandleRaftReady()
		}
	}
}

// 有临时保存 直接用，没有 去 rw 的 router 中找
func (rw *raftWorker) getPeerState(peersMap map[uint64]*peerState, regionID uint64) *peerState {
	peer, ok := peersMap[regionID]
	if !ok {
		peer = rw.pr.get(regionID)
		if peer == nil {
			return nil
		}
		peersMap[regionID] = peer
	}
	return peer
}
