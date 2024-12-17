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

	// 心跳周期(ms)
	HeartBeatInterval = 75
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
	applyCh chan ApplyMsg

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
	//
	// 当前server已提交的最高日志条目索引
	CommitIndex int

	// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	//
	// 已应用到本地状态机的最高日志条目的索引
	LastApplied int

	// ------------------------------------------------------------
	// VOLATILE STATE ON LEADERS: (Reinitialized after election)

	// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	//
	// 存储为每个follower准备发送的下一条日志条目的索引
	//
	// NextIndex[N] = x 表示下一次将向N节点发送索引为x的日志条目,
	// 如果节点N回复日志不一致,leader会将NextIndex[serverN]的值减一为x-1.
	NextIndex []int

	// for each server, index of the highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	//
	// 记录每个follower上已经复制了的最高日志条目索引，
	// 用于协助领导者判断某个日志条目是否已经在大多数节点上成功复制，
	// 从而决定是否提交日志.
	//
	// MatchIndex = [5, 5, 3, 3, 0] 表示索引为5的日志条目在节点0和1已经成功复制,
	// 索引为3的日志条目在节点2和3已经成功复制，
	// 节点4尚未复制任何日志
	MatchIndex []int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// # PrevLogIndex Leader希望follower复制的日志条目的前一个日志条目的索引
//
// # PreLogTerm PrevLogIndex所在的Term
//
// # Entries   Leader希望follower复制的日志条目
//
// # LeaderCommit Leader已提交的日志条目的最高索引
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int // new LogEntry的前一个LogEntry的Index
	PrevLogTerm  int // new LogEntry的前一个LogEntry的Term
	Entries      []LogEntry
	LeaderCommit int
}

// ConflictIndex  follower冲突的第一个索引
//
// 例如follower的日志中最后一个索引为 10，
// 而leader发送的日志起始索引为 12，则返回 ConflictIndex = 11。
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

// AppendEntries RPC Handler
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// fmt.Printf("server %d received AppendEntries from leader %d\n", rf.me, args.LeaderID)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("server %d's Logs:\n", rf.me)
	fmt.Println(rf.Logs)

	// 接收到AppendEntry的服务器的Term大于发送发送AppendEntry的Term，则拒绝确认领导权
	if rf.CurrentTerm > args.Term {
		fmt.Printf("server %d in term %d received AppendEntries from old term %d\n", rf.me, rf.CurrentTerm, args.Term)
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}

	// 接收到AppendEntry的服务器的Term小于等于发送AppendEntry的leader的Term，则加入新的Term
	// 或者同一任期的心跳来自的leader与当前server的VotedFor不同，则更改为新的leader
	if rf.CurrentTerm < args.Term || rf.VotedFor != args.LeaderID {
		rf.Donw2Follower4NewTerm(args.Term)
		// TODO rf.VotedFor应该置为-1还是args.LeaderID
		// rf.VotedFor = args.LeaderID
		rf.VotedFor = args.LeaderID
	}

	// 同一个Term里leader发来的AppendEntry RPC
	// args.Entrie != 0说明有要追加的日志条目
	// TODO 修改
	if len(args.Entries) != 0 {
		rf.Logs = append(rf.Logs, args.Entries...)
	}

	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = args.LeaderCommit
	}

	// args.Entrie == 0说明没有要追加的日志条目，视为简单的心跳测活
	rf.TimeStamp = time.Now()
	reply.Term = args.Term
	reply.Success = true
	fmt.Printf("server %d is alive -> leader %d\n", rf.me, args.LeaderID)
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// 不断尝试重连
	if !ok {
		if rf.dead {
			return false
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}
	return ok
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
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
	// fmt.Printf("server %d is persisting state\n", rf.me)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	err := e.Encode(rf)
	if err != nil {
		// fmt.Printf("持久层数据编码失败\n")
	}
	data := w.Bytes()
	if data == nil {
		// fmt.Printf("持久层数据为空，无法保存\n")
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
		// fmt.Printf("持久层数据为空，无法恢复\n")
		return
	}
	rf.applyCh <- ApplyMsg{}
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
	LastLogIndex int
	// term of candidate's last log entry
	lastLogTerm int
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

// TODO 根据Logs判断是否投票
// the voter denies its vote if its own log is more up-to-date than that of the candidate.
// Raft determines which of two logs is more up-to-date
// by comparing the index and term of the last entries in the logs.
// If the logs have last entries with different terms, then
// the log with the later term is more up-to-date. If the logs
// end with the same term, then whichever log is longer is
// more up-to-date.
//
// 处理投票请求
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// fmt.Printf("server %d received RequestVote from server %d\n", rf.me, args.CandidateID)

	rf.mu.Lock()
	defer rf.mu.Unlock()
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
		rf.VotedFor = args.CandidateID
	}

	// fmt.Printf("server %d's VotedFor is %d\n", rf.me, rf.VotedFor)

	// 为该server投票
	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateID {
		rf.VotedFor = args.CandidateID
		fmt.Printf("server %d votes for server %d\n", rf.me, args.CandidateID)
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
//
// 提交一条命令给server，如果该server是leader，就接收该命令并添加一条日志条目
//
// 参数：要提交的命令（日志条目的值）
//
// 返回值：包含该命令的日志条目的索引，当前任期，当前服务器是否认为自己是leader
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1

	if rf.dead {
		return -1, -1, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer func() {
	// 	fmt.Println("Start() returns")
	// }()
	term, isLeader := rf.GetState()

	// 该服务器不是leader
	if !isLeader {
		return -1, term, isLeader
	}

	// 新提交的命令（日志条目）在日志中的索引
	index = rf.MatchIndex[rf.me] + 1

	// 该服务器是leader，添加一条日志条目
	appendLogEntry := LogEntry{
		Command: command,
		Index:   index,
		Term:    term,
	}

	rf.Logs = append(rf.Logs, appendLogEntry)
	// 更新自己的信息
	// rf.MatchIndex[rf.me] = rf.Logs[len(rf.Logs)-1].Index
	// rf.NextIndex[rf.me] = rf.MatchIndex[rf.me] + 1
	fmt.Printf("COMMAND! leader %d receives a new command: %d\n", rf.me, command)

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
	rf.applyCh = applyCh

	// Your initialization code here.
	rf.mu.Lock()

	rf.Role = follower
	rf.VoteCnt = 0
	rf.VotedFor = -1 // -1表示没有投票
	rf.CurrentTerm = 0
	rf.dead = false
	// 0表示切片的初始长度为0
	rf.Logs = []LogEntry{
		{
			Command: nil,
			Index:   0,
			Term:    -1,
		},
	}
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))

	fmt.Printf("succeeded in initializing server %d\n", rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(rf.persister.ReadRaftState())
	rf.TimeStamp = time.Now()

	rf.mu.Unlock()

	// fmt.Printf("server %d's role is %d\n", rf.me, rf.Role)

	go rf.ticker()

	return rf
}

func (rf *Raft) ticker() {
	//  生成随机electionTimeout
	// time.Duration(n)返回n纳秒的时间间隔
	electionTimeout := getElectionTimeout()
	fmt.Printf("server %d set a electionTimeout = %d\n", rf.me, electionTimeout)

	for !rf.dead {
		// fmt.Printf("server %d's timeStamp is %s\n", rf.me, rf.timeStamp)
		rf.mu.Lock()
		if rf.dead {
			rf.mu.Unlock()
			return
		}
		elapsedTime := time.Since(rf.TimeStamp)
		curRole := rf.Role
		rf.mu.Unlock()

		switch curRole {
		case leader:
			if elapsedTime >= time.Duration(HeartBeatInterval)*time.Millisecond {
				fmt.Printf("leader %d's heartBeatTimeout ran out\n", rf.me)
				// 更新时间戳
				rf.mu.Lock()
				rf.TimeStamp = time.Now()

				// 发送心跳
				go rf.HeartBeat()
				rf.mu.Unlock()
			}

		case follower:
			if elapsedTime >= time.Duration(electionTimeout)*time.Millisecond {
				// fmt.Printf("server %d's electionTimeout ran out\n", rf.me)
				rf.mu.Lock()
				rf.Role = candidate
				rf.CurrentTerm += 1
				// 更新时间戳
				rf.TimeStamp = time.Now()
				rf.mu.Unlock()
				// 开始选举
				go rf.election()

				// 重置选举超时
				electionTimeout = getElectionTimeout()
				// fmt.Printf("server %d reset a electionTimeout = %d\n", rf.me, electionTimeout)
			}
		case candidate:
			if elapsedTime >= time.Duration(electionTimeout)*time.Millisecond {
				// fmt.Printf("server %d's electionTimeout ran out\n", rf.me)
				// 更新时间戳
				rf.mu.Lock()
				rf.CurrentTerm += 1
				rf.TimeStamp = time.Now()
				rf.mu.Unlock()
				// 开始选举
				go rf.election()

				// 重置选举超时
				electionTimeout = getElectionTimeout()
				// fmt.Printf("server %d reset a electionTimeout = %d\n", rf.me, electionTimeout)
			}
		}
	}
}

func (rf *Raft) election() {

	rf.mu.Lock()
	if rf.Role != candidate {
		rf.mu.Unlock()
		return
	}

	// 持久化存储状态
	// fmt.Printf("server %d starting a new election\n", rf.me)
	// rf.Role = candidate
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
		// 已经不是candidate
		rf.mu.Lock()
		if rf.Role != candidate {
			fmt.Printf("server %d is not a candidate anymore, quit election\n", rf.me)
			rf.mu.Unlock()
			break
		}
		// 已经是leader，停止发送投票请求
		if rf.Role == leader {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		go func(idx int) {
			reqReply := RequestVoteReply{}
			// rf.mu.Lock()
			// defer rf.mu.Unlock()
			// fmt.Printf("server %d is sending requestVote to server %d\n", rf.me, idx)
			ok := rf.sendRequestVote(idx, reqArgs, &reqReply)
			if !ok {
				fmt.Printf("server %d couldn'g be contacted with requestVote\n", idx)
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 收到来自更新的Term的投票请求，投票给他
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
		}(i)
	}
}

// AppendEntries 的处理过程也包含在 HeartBeat 中。
func (rf *Raft) HeartBeat() {

	rf.mu.Lock()

	// 非leader则立即退出
	if rf.Role != leader {
		rf.mu.Unlock()
		return
	}

	rf.TimeStamp = time.Now()
	// 更新自己的信息
	rf.MatchIndex[rf.me] = rf.Logs[len(rf.Logs)-1].Index
	rf.NextIndex[rf.me] = rf.MatchIndex[rf.me] + 1
	fmt.Println(rf.NextIndex)
	// 持久化存储状态
	rf.persist()
	rf.mu.Unlock()

	// fmt.Printf("leader %d sending heartbeat to all servers\n", rf.me)

	for i := 0; i < len(rf.peers); i++ {

		if i == rf.me {
			continue
		}

		// TODO 这部分是否许需要判断是不是leader？
		rf.mu.Lock()
		// 不是leader则立即停止发送RPC
		if rf.Role != leader {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		go func(idx int) {

			// leader已被终结
			if rf.dead {
				return
			}
			// 普通心跳，则appendLogEntries为空
			appendLogEntries := []LogEntry{}

			rf.mu.Lock()
			fmt.Println(rf.Logs)

			// paper figure 2 rules for servers(leaders)
			// If last log index ≥ nextIndex for a follower: send
			// AppendEntries RPC with log entries starting at nextIndex
			// leader的最后的日志条目的索引大于该follower的下一个日志条目索引
			// 说明有新的LogEntry需要发送
			// rf.Logs[len(rf.Logs)-1].Index即为leader中最后的LogEntry的索引
			if rf.Logs[len(rf.Logs)-1].Index >= rf.NextIndex[idx] {
				fmt.Println("This time's appendEntries is not empty")
				// 构造要发送的LogEntries，长度为
				appendLogEntries = make([]LogEntry, len(rf.Logs[rf.NextIndex[idx]-1:]))
				// 将要发送的LogEntries复制到appendLogEntries中
				copy(appendLogEntries, rf.Logs[rf.NextIndex[idx]-1:])
			}
			// fmt.Println(appendLogEntries)
			// 构造RPC参数
			// rf.Logs[rf.NextIndex[idx]-rf.Logs[len(rf.Logs)-1].Index-1]为
			// 要追加给follower的LogEntries的前一条LogEntry
			// fmt.Printf("%d, %d\n", rf.NextIndex[idx], rf.Logs[len(rf.Logs)-1].Index)
			aeArgs := AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: rf.Logs[rf.NextIndex[idx]-1].Index,
				PrevLogTerm:  rf.Logs[rf.NextIndex[idx]-1].Term,
				Entries:      appendLogEntries,
				LeaderCommit: rf.CommitIndex,
			}
			rf.mu.Unlock()

			aeReply := AppendEntriesReply{}
			// fmt.Printf("leader %d send heartbeat to server %d\n", rf.me, idx)
			// fmt.Printf("%d\n", idx)
			ok := rf.sendAppendEntries(idx, aeArgs, &aeReply)

			if !ok {
				fmt.Printf("server %d couldn'g be contacted with heartbeat\n", idx)
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 如果已经不是leader，直接返回
			// 如果任期已更新，则不处理就任期的RPC响应，直接返回
			if rf.Role != leader || rf.CurrentTerm != aeArgs.Term {
				return
			}

			// 目标server拒绝确认领导权
			// 如果有更新的任期存在，直接返回，等待新leader发送的心跳
			if !aeReply.Success {
				// rf.Donw2Follower4NewTerm(aeReply.Term)
				return
			}

			//  TODO 考虑如何处理冲突，不能仅凭 len(aeArgs.Entries) == 0 就认为是简单的心跳测活，还要检测有没有冲突
			if len(aeArgs.Entries) != 0 {
				// AppendEntries成功
				// fmt.Printf("rf.NextIndex[%d]: %d, len(aeArgs.Entries): %d\n", idx, rf.NextIndex[idx], len(aeArgs.Entries))
				expectedMatchIdx := rf.MatchIndex[rf.me]
				// 更新leader储存的followers的信息
				rf.MatchIndex[idx] = max(rf.MatchIndex[idx], expectedMatchIdx)
				rf.NextIndex[idx] = rf.MatchIndex[idx] + 1
				// TODO 判断是否更新CommitIndex

				// 简易版本
				replica_cnt := 0
				for i := 0; i < len(rf.peers); i++ {
					if rf.MatchIndex[i] >= rf.CommitIndex {
						replica_cnt++
					}
				}
				if replica_cnt > len(rf.peers)/2 {
					rf.CommitIndex += len(aeArgs.Entries)
					fmt.Printf("提交索引为 %d 的日志\n", rf.CommitIndex)
					rf.Apply(rf.CommitIndex, rf.Logs[rf.CommitIndex].Command)
				}
			}
		}(i)
	}
}

// 接收到最新Term的消息，以follower身份加入新Term
// 在该函数的上下文要有互斥锁包围
func (rf *Raft) Donw2Follower4NewTerm(NewTerm int) {
	rf.CurrentTerm = NewTerm
	rf.Role = follower
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
	// 成为leader后立刻发送一次心跳
	// go rf.HeartBeat()

	// 初始化NextIndex和MatchIndex
	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		// 初始化为leader的最后一个日志条目的索引加1
		rf.NextIndex[i] = rf.Logs[len(rf.Logs)-1].Index + 1
	}
	for i := 0; i < len(rf.peers); i++ {
		// 初始化为0
		rf.MatchIndex[i] = 0
	}
}

func (rf *Raft) Apply(Index int, Command interface{}) {
	applyMsg := ApplyMsg{
		Index:   Index,
		Command: Command,
	}
	rf.applyCh <- applyMsg
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

// func (rf *Raft) getPrevLogIndex() {
// }

// func (rf *Raft) getPrevLogTerm() {
// }
