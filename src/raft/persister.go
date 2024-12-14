package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"fmt"
	"sync"
)

/*
Persister提供持久化存储服务，用于保存和恢复 Raft 节点的状态数据。
Raft 协议中的一个关键问题是如何保证数据持久性，以确保在节点崩溃或重启后可以恢复其状态。
*/
type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	/*
		快照是为了优化日志存储和处理而引入的一种机制，它保存了状态机的当前状态。
		使用快照的目的是避免保存每个日志条目，从而节省存储空间并提高性能。
	*/
	snapshot []byte
}

// 创建一个Persister
func MakePersister() *Persister {
	return &Persister{}
}

// 复制一个Persister
func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

// 以stream形式保存raft状态
func (ps *Persister) SaveRaftState(data []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if data == nil {
		fmt.Println("WARNING: 要持久化的数据为空")
	}
	fmt.Println("正在持久化存储数据...")
	ps.raftstate = data
}

// 以stream读取raft状态并返回
func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	fmt.Println("正在读取持久化数据...")
	return ps.raftstate
}

// raft状态大小
func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// 保存raft快照
func (ps *Persister) SaveSnapshot(snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.snapshot = snapshot
}

// 读取raft快照
func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.snapshot
}
