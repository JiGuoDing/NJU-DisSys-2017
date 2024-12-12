
### 

`RequestVoteArgs` 结构体定义

```go
type RequestVoteArgs struct {
	// candidate's term
	term int
	// candidate requesting vote
	candidateID int
	// index of candidate's last log entry
	lastLogIndex int
	// term of candidate's last log entry
	lastLogTerm int
}
```

`RequestVoteReply` 结构体定义

```go
type RequestVoteReply struct {
	// currentTerm, for candidate to update itself
	term int
	// true means candidate received vote
	voteGranted bool
}
```

`RaftState` 结构体定义
```go
type RaftState struct {
	// latest term server has seen (initialized to 0 on first boot, increases monotonically)
	currentTerm int

	// candidateId that received a vote in current term (or null if none)
	votedFor int

	// log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	log []string

	// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	commitIndex int

	// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	lastApplied int

	// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	nextIndex []int

	// for each server, index of the highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	matchIndex []int
}
```
