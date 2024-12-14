package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"disEx02.jgd/src/labrpc"
)

// import "bytes"
// import "encoding/gob"

// 常量定义
const (
	// Raft server的角色
	leader    = 0
	candidate = 1
	follower  = 2

	// 选举超时时间上下限（ms）
	MinElectionTimeout = 150
	MaxElectionTimeout = 300

	// 心跳周期
	HeartBeatInterval = 50
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
// ApplyMsg是Raft节点与上层应用之间通信的桥梁。
// 确保了日志条目的正确执行和状态的同步
type ApplyMsg struct {
	// 日志条目索引
	Index int
	// 实际的命令
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu sync.Mutex
	// 存储所有Raft节点的客户端连接
	peers []*labrpc.ClientEnd
	// 用于持久化Raft节点的状态
	persister *Persister
	// 当前节点在peers列表中的索引
	me int // index into peers[]
	// 是否被终结
	dead bool

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 该raft server的状态
	RaftState
}

type RaftState struct {
	// PERSISTENT STATE ON ALL SERVERS: (Updated on stable storage before responding to RPCs)
	// 回复RPCs之前要先持久化存储起来，防止crash或者restart后需要重新读取

	// latest term server has seen (initialized to 0 on first boot, increases monotonically)
	CurrentTerm int

	// candidateId that received a vote in current term (or null if none)
	VotedFor int

	// log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	Logs []LogEntry

	// ------------------------------------------------------------
	// VOLATILE STATE ALL SERVERS:

	// the role this raft server is currently in
	Role int

	// the count of votes received from self and other servers
	VoteCnt int

	// the time to record
	TimeStamp time.Time

	// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	// commitIndex int

	// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	// lastApplied int

	// ------------------------------------------------------------
	// VOLATILE STATE ON LEADERS: (Reinitialized after election)

	// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	// nextIndex []int

	// for each server, index of the highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	// matchIndex []int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderID int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	fmt.Printf("server %d received AppendEntries from leader %d\n", rf.me, args.LeaderID)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 接收到AppendEntry的服务器的Term大于发送发送AppendEntry的Term，则拒绝确认领导权
	if rf.CurrentTerm > args.Term {
		fmt.Printf("server %d in term %d received AppendEntries from old term %d\n", rf.me, rf.CurrentTerm, args.Term)
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}

	// 接收到AppendEntry的服务器的Term小于发送AppendEntry的server的Term，则加入新的Term
	if rf.CurrentTerm < args.Term {
		rf.Donw2Follower4NewTerm(args.Term)
	}

	// 同一个Term里leader发来的AppendEntry则视为测活心跳
	rf.TimeStamp = time.Now()
	reply.Term = args.Term
	reply.Success = true
	fmt.Printf("server %d is alive -> leader %d\n", rf.me, args.LeaderID)
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = rf.Role == leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	fmt.Printf("server %d is persisting state\n", rf.me)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	err := e.Encode(rf)
	if err != nil {
		fmt.Printf("持久层数据编码失败\n")
	}
	data := w.Bytes()
	if data == nil {
		fmt.Printf("持久层数据为空，无法保存\n")
		return
	}
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
//
// 以stream形式读入，需要解码器解码
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:

	if data == nil { // bootstrap without any state?
		fmt.Printf("持久层数据为空，无法恢复\n")
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	fmt.Printf("server %d is reloading persistent state\n", rf.me)

	// 解码出持久化状态
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Logs)

	fmt.Printf("server %d 's reloaded term is \n", rf.CurrentTerm)
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.

	// candidate's term
	Term int
	// candidate requesting vote
	CandidateID int
	// index of candidate's last log entry
	// LastLogIndex int
	// term of candidate's last log entry
	// lastLogTerm int
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here.

	// currentTerm, for candidate to update itself
	Term int
	// true means candidate received vote
	VoteGranted bool
}

// example RequestVote RPC handler.
// func (rf *Raft) RequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) {
// 	// Your code here.
// 	ok := rf.sendRequestVote(server, args, reply)
// 	// false means the server couldn't be contacted
// 	if !ok {
// 		return
// 	}

// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	// 收到更新的Term的消息，以follower身份加入新Term
// 	if reply.Term > rf.currentTerm {
// 		rf.Donw2Follower4NewTerm(reply.Term)
// 		return
// 	}

// 	// 目标server拒绝投票或者当前server非candidate
// 	if !reply.VoteGranted || rf.role != candidate {
// 		return
// 	}

// 	// 票数加1
// 	rf.voteCnt++

// 	if rf.role == leader {
// 		return
// 	}

// 	// 不是leader且获得多余半数的票，成为新Term的leader
// 	if rf.voteCnt > len(rf.peers)/2 {
// 		rf.role = leader
// 		fmt.Printf("server %d becomes the new leader of term %d\n", rf.me, rf.currentTerm)
// 	}
// }

// 处理投票请求
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("server %d received RequestVote from server %d\n", rf.me, args.CandidateID)
	reply.Term = rf.CurrentTerm

	// 收到来自过时Term的投票请求
	if args.Term < rf.CurrentTerm {
		fmt.Printf("server %d rejects RequestVote from server %d because it's term is outdated\n", rf.me, args.CandidateID)
		reply.VoteGranted = false
		return
	}

	// 收到来自更新的Term的投票请求（存在更新的Term）
	if args.Term > rf.CurrentTerm {
		fmt.Printf("server %d received RequestVote from server %d with newer term %d\n", rf.me, args.CandidateID, args.Term)
		// 加入新Term
		rf.Donw2Follower4NewTerm(args.Term)
		rf.TimeStamp = time.Now()
	}

	// 为该server投票
	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateID {
		rf.VotedFor = args.CandidateID
		reply.VoteGranted = true
		rf.Role = follower
		rf.TimeStamp = time.Now()
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
	fmt.Printf("server %d is killed\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.dead = true
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.mu.Lock()
	rf.Role = follower
	rf.VoteCnt = 0
	rf.VotedFor = -1 // -1表示没有投票
	rf.CurrentTerm = 0
	rf.Logs = append(rf.Logs, LogEntry{
		Term:    0,
		Command: nil,
	})

	fmt.Printf("succeeded in initializing server %d\n", rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(rf.persister.ReadRaftState())
	rf.TimeStamp = time.Now()

	fmt.Printf("server %d's role is %d\n", rf.me, rf.Role)

	rf.mu.Unlock()

	go rf.ticker()

	return rf
}

func (rf *Raft) ticker() {
	//  生成随机electionTimeout
	// time.Duration(n)返回n纳秒的时间间隔
	electionTimeout := getElectionTimeout()
	fmt.Printf("server %d set a electionTimeout = %d\n", rf.me, electionTimeout)

	for {
		// fmt.Printf("server %d's timeStamp is %s\n", rf.me, rf.timeStamp)
		rf.mu.Lock()
		if rf.dead {
			return
		}
		elapsedTime := time.Since(rf.TimeStamp)
		rf.mu.Unlock()

		switch rf.Role {
		case leader:
			if elapsedTime >= time.Duration(HeartBeatInterval)*time.Millisecond {
				fmt.Printf("leader %d's heartBeatTimeout ran out\n", rf.me)
				// 更新时间戳
				rf.mu.Lock()
				rf.TimeStamp = time.Now()
				rf.mu.Unlock()

				// 发送心跳
				go rf.HeartBeat()
			}

			// TODO 是否需要锁在这里？ for循环可能多次执行
		case follower, candidate:
			if elapsedTime >= time.Duration(electionTimeout)*time.Millisecond {
				// 更新时间戳
				rf.mu.Lock()
				rf.TimeStamp = time.Now()
				rf.mu.Unlock()

				fmt.Printf("server %d's electionTimeout ran out\n", rf.me)
				// 开始选举
				go rf.election()
			}
		}
	}
}

func (rf *Raft) election() {

	// 持久化存储状态
	rf.mu.Lock()
	fmt.Printf("server %d starting a new election\n", rf.me)
	rf.Role = candidate
	rf.CurrentTerm += 1
	rf.VotedFor = rf.me
	rf.VoteCnt = 1
	rf.TimeStamp = time.Now()
	rf.persist()
	rf.mu.Unlock()

	reqArgs := RequestVoteArgs{
		Term:        rf.CurrentTerm,
		CandidateID: rf.me,
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func() {
			reqReply := RequestVoteReply{}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			fmt.Printf("server %d is sending requestVote to server %d\n", rf.me, i)
			ok := rf.sendRequestVote(i, reqArgs, &reqReply)
			if !ok {
				fmt.Printf("server %d couldn'g be contacted with requestVote\n", i)
				return
			}

			if reqReply.Term > rf.CurrentTerm {
				rf.Donw2Follower4NewTerm(reqReply.Term)
				return
			}

			if !reqReply.VoteGranted || rf.Role != candidate {
				return
			}

			rf.VoteCnt++

			if rf.Role == leader {
				return
			}

			if rf.VoteCnt > len(rf.peers)/2 {
				rf.Up2Leader()
			}
		}()
	}
}

func (rf *Raft) HeartBeat() {

	// 持久化存储状态
	rf.mu.Lock()
	rf.TimeStamp = time.Now()
	rf.persist()
	rf.mu.Unlock()

	aeArgs := AppendEntriesArgs{
		LeaderID: rf.me,
		Term:     rf.CurrentTerm,
	}

	fmt.Printf("leader %d sending heartbeat to all servers\n", rf.me)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			aeReply := AppendEntriesReply{}
			fmt.Printf("leader %d send heartbeat to server %d\n", rf.me, i)
			ok := rf.sendAppendEntries(i, aeArgs, &aeReply)

			if !ok {
				fmt.Printf("server %d couldn'g be contacted with heartbeat\n", i)
				return
			}

			// 目标server拒绝确认领导权，存在新Term，以follower身份加入新Term
			if !aeReply.Success {
				rf.Donw2Follower4NewTerm(aeReply.Term)
				return
			}
		}()
	}
}

// 接收到最新Term的消息，以follower身份加入新Term
// 在该函数的上下文要有互斥锁包围
func (rf *Raft) Donw2Follower4NewTerm(NewTerm int) {
	rf.CurrentTerm = NewTerm
	rf.Role = follower
	rf.VotedFor = -1
	rf.VoteCnt = 0
	rf.TimeStamp = time.Now()
	fmt.Printf("server %d joins term %d as follower\n", rf.me, NewTerm)
	rf.persist()
}

// 成为leader
func (rf *Raft) Up2Leader() {
	if rf.Role != candidate {
		return
	}
	rf.Role = leader
	rf.TimeStamp = time.Now()
	fmt.Printf("server %d becomes leader of term %d\n", rf.me, rf.CurrentTerm)
	go rf.HeartBeat()
}

// 生成随机选举超时时间
func getElectionTimeout() int {
	return rand.Intn(MaxElectionTimeout-MinElectionTimeout) + MinElectionTimeout
}

// 更新时间戳
// func (rf *Raft) updateTimeStamp() {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	rf.TimeStamp = time.Now()
// }
