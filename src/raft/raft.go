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
	"fmt"
	"labgob"
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	UseSnapshot  bool   // ignore for lab2; only used in lab3
	Snapshot     []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type State int

const (
	Follower  State = 0
	Candidate       = 1
	Leader          = 2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry
	state       State

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	resetChan  chan int
	commitCond *sync.Cond
	applyCh    chan ApplyMsg
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

func (rf *Raft) GetLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) GetLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	rf.persister.SaveRaftState(rf.getStateToPersist())
}

func (rf *Raft) getStateToPersist() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		fmt.Printf("error decoding\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

func (rf *Raft) readSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}

	rf.commitIndex = rf.log[0].Index
	rf.lastApplied = rf.log[0].Index

	msg := ApplyMsg{UseSnapshot: true, Snapshot: data}
	go func() {
		rf.applyCh <- msg
	}()
}

func truncateLog(lastIncludedIndex int, lastIncludedTerm int, log []LogEntry) []LogEntry {
	var newLog []LogEntry
	newLog = append(newLog, LogEntry{Index: lastIncludedIndex, Term: lastIncludedTerm})
	for index := len(log) - 1; index >= 0; index-- {
		if log[index].Index == lastIncludedIndex && log[index].Term == lastIncludedTerm {
			newLog = log[index:]
			break
		}
	}
	return newLog
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	ConflictTerm int
	FirstIndex   int
}
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		fmt.Printf("No vote for server %v by server %v: old term\n", args.CandidateID, rf.me)
		return
	}
	rf.updateTerm(args.Term)
	if rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		// already voted for other
		fmt.Printf("No vote for server %v by server %v: already voted for %v\n", args.CandidateID, rf.me, rf.votedFor)
		return
	}

	lastLog := rf.log[len(rf.log)-1]
	if args.LastLogTerm > lastLog.Term ||
		args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.persist()
		fmt.Printf("Vote for server %v by server %v\n", args.CandidateID, rf.me)
		go func() {
			rf.resetChan <- 0
		}()
		return
	}
	fmt.Printf("No vote for server %v by server %v: candidate: {%v, %v}, me: {%v, %v}\n", args.CandidateID, rf.me, args.LastLogTerm, args.LastLogIndex, lastLog.Term, len(rf.log)-1)
}

//
// AppendEntries
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	baseIndex := rf.log[0].Index

	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}
	rf.updateTerm(args.Term)
	go func() {
		rf.resetChan <- 0
	}()
	if args.PrevLogIndex > rf.GetLastLogIndex() {
		reply.FirstIndex = rf.GetLastLogIndex() + 1
		return
	}
	if args.PrevLogIndex < baseIndex {
		return
	}
	if rf.log[args.PrevLogIndex-baseIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex-baseIndex].Term
		for i := args.PrevLogIndex; i >= baseIndex; i-- {
			if rf.log[i-baseIndex].Term != reply.ConflictTerm {
				reply.FirstIndex = i + 1
				break
			}
		}
		return
	}
	reply.Success = true
	// append logs
	if args.Entries != nil && len(args.Entries) > 0 {
		for i := range args.Entries {
			index := args.PrevLogIndex + 1 + i
			if index > rf.GetLastLogIndex() || // new entries, append directly
				args.Entries[i].Term != rf.log[index-baseIndex].Term { // same index with different term, delete existing entries
				rf.log = append(rf.log[:args.PrevLogIndex+1+i-baseIndex], args.Entries[i:]...)
				rf.persist()
				break
			}
		}
		fmt.Printf("server %v log len %v = %v + %v\n", rf.me, len(rf.log), args.PrevLogIndex+1, len(args.Entries))
	}
	if args.LeaderCommit > rf.commitIndex {
		oldCommiIndex := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, rf.GetLastLogIndex())
		if rf.commitIndex != oldCommiIndex {
			fmt.Printf("server %v commit index %v -> %v, leader commit %v, log len %v\n", rf.me, oldCommiIndex, rf.commitIndex, args.LeaderCommit, len(rf.log))
			go func() {
				rf.commitCond.L.Lock()
				rf.commitCond.Signal()
				rf.commitCond.L.Unlock()
			}()
		}
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.updateTerm(args.Term)
	go func() {
		rf.resetChan <- 0
	}()
	rf.log = truncateLog(args.LastIncludedIndex, args.LastIncludedTerm, rf.log)
	fmt.Printf("follower %v log len after truncated: %v, baseIndex=%v lastApplied=%v\n", rf.me, len(rf.log), rf.log[0].Index, args.LastIncludedIndex)
	msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.persister.SaveStateAndSnapshot(rf.getStateToPersist(), args.Data)
	go func() {
		rf.applyCh <- msg
	}()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendRequestVoteToAll() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader {
		return
	}
	fmt.Printf("Start election: server %v\n", rf.me)

	// convert to candidate state
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = Candidate
	rf.persist()

	args := &RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateID = rf.me
	args.LastLogTerm = rf.GetLastLogTerm()
	args.LastLogIndex = rf.GetLastLogIndex()

	voteTerm := rf.currentTerm // used to check out-dated response
	votes := 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, args, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if voteTerm != rf.currentTerm { // out-dated response, ignore it
				return
			}
			if rf.updateTerm(reply.Term) { // convert to follower
				return
			}
			if reply.VoteGranted {
				votes++
				fmt.Printf("Vote=%v for server %v\n", votes, rf.me)
			}
			if votes > len(rf.peers)/2 && rf.state != Leader {
				// convert to leader
				fmt.Printf("Become leader: server %v\n", rf.me)
				rf.state = Leader
				for i := range rf.peers {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
				}
				go func() {
					for {
						if !rf.sendHeartbeatToAll() {
							return
						}
						time.Sleep(100 * time.Millisecond)
					}
				}()
			}
		}(i)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeatToAll() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return false
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := &AppendEntriesArgs{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex = rf.GetLastLogIndex()
		args.PrevLogTerm = rf.GetLastLogTerm()
		args.LeaderCommit = rf.commitIndex
		requestTerm := rf.currentTerm

		rf.startHelper(i, requestTerm, args, false)
	}
	return true
}

func (rf *Raft) updateTerm(term int) bool {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
		return true
	}
	return false
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.log[0].Index
	index = rf.GetLastLogIndex() + 1
	term = rf.currentTerm
	isLeader = rf.state == Leader

	if isLeader {
		fmt.Printf("start %v\n", command)
		rf.log = append(rf.log, LogEntry{index, term, command})
		rf.persist()
		// start agreement
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				rf.matchIndex[i] = rf.GetLastLogIndex()
				continue
			}
			args := &AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			if rf.nextIndex[i] > baseIndex {
				args.PrevLogTerm = rf.log[args.PrevLogIndex-baseIndex].Term
				args.Entries = rf.log[rf.nextIndex[i]-baseIndex:]
			}
			args.LeaderCommit = rf.commitIndex

			requestTerm := rf.currentTerm // used to check out-dated response
			rf.startHelper(i, requestTerm, args, rf.nextIndex[i] <= baseIndex)
		}
	}

	return index, term, isLeader
}

func (rf *Raft) startHelper(server int, requestTerm int, args *AppendEntriesArgs, sendSnapshot bool) {
	go func() {
		// log truncated. Need to send snapshot
		if sendSnapshot {
			rf.mu.Lock()
			args1 := &InstallSnapshotArgs{}
			args1.Term = rf.currentTerm
			args1.LeaderId = rf.me
			args1.LastIncludedIndex = rf.log[0].Index
			args1.LastIncludedTerm = rf.log[0].Term
			args1.Data = rf.persister.ReadSnapshot()
			rf.mu.Unlock()

			reply1 := &InstallSnapshotReply{}
			if !rf.sendInstallSnapshot(server, args1, reply1) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.updateTerm(reply1.Term) {
				return
			}
			rf.nextIndex[server] = args1.LastIncludedIndex + 1
			rf.matchIndex[server] = args1.LastIncludedIndex
			return
		}

		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, args, reply)
		if !ok {
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		baseIndex := rf.log[0].Index
		if requestTerm != rf.currentTerm { // out-dated response, ignore it
			return
		}
		if rf.updateTerm(reply.Term) {
			return
		}

		if reply.Success {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		} else {
			rf.nextIndex[server] = reply.FirstIndex
			// if rf.nextIndex[server] > 1 {
			// 	rf.nextIndex[server]--
			// }
			args.PrevLogIndex = rf.nextIndex[server] - 1
			if args.PrevLogIndex >= baseIndex {
				args.PrevLogTerm = rf.log[args.PrevLogIndex-baseIndex].Term
				args.Entries = rf.log[rf.nextIndex[server]-baseIndex:]
			}
			// retry
			rf.startHelper(server, requestTerm, args, args.PrevLogIndex < baseIndex)
		}
		temp := make([]int, len(rf.matchIndex))
		copy(temp, rf.matchIndex)
		sort.Ints(temp)
		N := temp[len(temp)/2]
		if N > rf.commitIndex && rf.log[N-baseIndex].Term == rf.currentTerm {
			rf.commitIndex = N
			go func() {
				rf.commitCond.L.Lock()
				rf.commitCond.Signal()
				rf.commitCond.L.Unlock()
			}()
		}
	}()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
func (rf *Raft) StartSnapshot(snapshot []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.log[0].Index
	if index < baseIndex || index > rf.GetLastLogIndex() {
		return
	}
	rf.log = rf.log[index-baseIndex:]
	fmt.Printf("server %v log len after truncated: %v\n", rf.me, len(rf.log))

	state := rf.getStateToPersist()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.votedFor = -1 // no vote initially
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{0, 0, nil}
	rf.resetChan = make(chan int)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	m := &sync.Mutex{}
	rf.commitCond = sync.NewCond(m)

	go func() {
		for {
			electionTimeout := 800 + rand.Intn(400)
			select {
			case <-rf.resetChan:
				// continue
			case <-time.After(time.Duration(electionTimeout) * time.Millisecond):
				rf.sendRequestVoteToAll()
			}
		}
	}()

	go func() {
		for {
			rf.commitCond.L.Lock()
			for {
				rf.mu.Lock()
				cond := rf.lastApplied < rf.commitIndex
				rf.mu.Unlock()
				if cond {
					break
				}
				rf.commitCond.Wait()
			}
			rf.mu.Lock()
			index := rf.lastApplied + 1
			if index > rf.GetLastLogIndex() {
				fmt.Printf("server %v try to send index %v, log len is %v\n", rf.me, index, len(rf.log))
				// cond might signal many times. Need to check index.
				// another solution is to only signal when commit index actually changed
				rf.mu.Unlock()
				rf.commitCond.L.Unlock()
				continue
			}
			baseIndex := rf.log[0].Index
			if index < baseIndex {
				// already in snapshot, no need to apply
				rf.mu.Unlock()
				rf.commitCond.L.Unlock()
				continue
			}
			msg := ApplyMsg{CommandValid: true, Command: rf.log[index-baseIndex].Command, CommandIndex: index}
			fmt.Printf("server %v send apply msg {%v: %v}\n", rf.me, index, rf.log[index-baseIndex].Command)
			rf.lastApplied++
			rf.mu.Unlock()

			applyCh <- msg
			rf.commitCond.L.Unlock()
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	return rf
}
