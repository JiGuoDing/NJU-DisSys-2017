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
	currentTerm int

	// candidateId that received a vote in current term (or null if none)
	votedFor int

	// log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	logs []LogEntry

	// ------------------------------------------------------------
	// VOLATILE STATE ALL SERVERS:

	// the role this raft server is currently in
	role int

	// the count of votes received from self and other servers
	voteCnt int

	// the time to record
	timeStamp time.Time

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
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
//
// 以stream形式读入，需要解码器解码
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	// 解码出持久化状态
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
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
	reply.Term = rf.currentTerm

	// 收到来自过时Term的投票请求
	if args.Term < rf.currentTerm {
		fmt.Printf("server %d rejects RequestVote from server %d because it's term is outdated\n", rf.me, args.CandidateID)
		reply.VoteGranted = false
		return
	}

	// 收到来自更新的Term的投票请求（存在更新的Term）
	if args.Term > rf.currentTerm {
		fmt.Printf("server %d receives RequestVote from server %d with newer term %d\n", rf.me, args.CandidateID, args.Term)
		// 加入新Term
		rf.Donw2Follower4NewTerm(args.Term)
	}

	// 为该server投票
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		rf.role = follower
		rf.timeStamp = time.Now()
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
	rf.role = follower
	rf.voteCnt = 0
	rf.votedFor = -1 // -1表示没有投票
	rf.currentTerm = 0
	rf.timeStamp = time.Now()
	rf.logs = append(rf.logs, LogEntry{
		Term:    0,
		Command: nil,
	})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()

	return rf
}

func (rf *Raft) ticker() {
	//  生成随机electionTimeout
	// time.Duration(n)返回n纳秒的时间间隔
	electionTimeout := getElectionTimeout()
	fmt.Printf("server %d set a electionTimeout = %d\n", rf.me, electionTimeout)

	for {
		rf.mu.Lock()
		if elapsedTime := time.Since(rf.timeStamp); rf.role != leader && elapsedTime >= time.Duration(electionTimeout)*time.Millisecond {
			fmt.Printf("server %d's electionTimeout ran out, starting a new election\n", rf.me)
			// 开始选举
			go rf.election()
		}
		rf.mu.Unlock()

		// 重置electionTimeout
		electionTimeout = getElectionTimeout()
		// 随机延迟一段时间再开始选举延时流动(50-200ms)
		time.Sleep(time.Duration(rand.Intn(150)+50) * time.Millisecond)
	}
}

func (rf *Raft) election() {

	rf.mu.Lock()
	fmt.Printf("server %d starting a new election\n", rf.me)
	rf.role = candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.voteCnt = 1
	rf.timeStamp = time.Now()
	rf.persist()
	rf.mu.Unlock()

	reqArgs := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
	}

	for i := 0; i < len(rf.peers); i++ {
		rf.mu.Lock()
		if i == rf.me {
			rf.mu.Unlock()
			continue
		}
		reqReply := RequestVoteReply{}
		go func() {
			defer rf.mu.Unlock()
			ok := rf.sendRequestVote(i, reqArgs, &reqReply)
			if !ok {
				fmt.Printf("server %d couldn'g be contacted\n", i)
				return
			}

			if reqReply.Term > rf.currentTerm {
				rf.Donw2Follower4NewTerm(reqReply.Term)
				return
			}

			if !reqReply.VoteGranted || rf.role != candidate {
				return
			}

			rf.voteCnt++

			if rf.role == leader {
				return
			}

			if rf.voteCnt > len(rf.peers)/2 {
				rf.Up2Leader()

			}
		}()
	}
}

// 接收到最新Term的消息，以follower身份加入新Term
func (rf *Raft) Donw2Follower4NewTerm(NewTerm int) {
	rf.currentTerm = NewTerm
	rf.role = follower
	rf.votedFor = -1
	rf.voteCnt = 0
	rf.timeStamp = time.Now()
	fmt.Printf("server %d joins term %d as follower\n", rf.me, NewTerm)
	rf.persist()
}

// 成为leader
func (rf *Raft) Up2Leader() {
	if rf.role != candidate {
		return
	}
	rf.role = leader
	rf.timeStamp = time.Now()
	fmt.Printf("server %d becomes leader of term %d\n", rf.me, rf.currentTerm)
}

// 生成随机选举超时时间
func getElectionTimeout() int {
	return rand.Intn(MaxElectionTimeout-MinElectionTimeout) + MinElectionTimeout
}
