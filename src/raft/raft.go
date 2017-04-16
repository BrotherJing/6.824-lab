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

import "sync"
import "labrpc"

// import "bytes"
// import "encoding/gob"

import "time"
import "math/rand"
//import "fmt"

type LogEntry struct{
	Term	int
	Command	interface{}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm	int
	voteFor		int

	log			[]LogEntry
	commitIndex	int
	lastApplied	int
	nextIndex	[]int
	matchIndex	[]int

	resetChan	chan int
	role		int//0: follower, 1: candidate, 2: leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	//isleader = (rf.voteFor == rf.me)
	isleader = (rf.role == 2)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term		int
	VoteGranted	bool
}

//
// AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	Term			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	entries			[]LogEntry
	LeaderCommit	int
}

//
// AppendEntries RPC arguments structure.
//
type AppendEntriesReply struct {
	Term	int
	Success	bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm{
		reply.VoteGranted = false
	}else if args.Term == rf.currentTerm && rf.voteFor != -1 && rf.voteFor != args.CandidateId{
		reply.VoteGranted = false
	}else{
		reply.VoteGranted = true
		if args.Term > rf.currentTerm{
			rf.currentTerm = args.Term
		}
		rf.role = 0
		rf.voteFor = args.CandidateId
		go func(){
			//fmt.Printf("Raft %v votes for Raft %v at term %v\n", rf.me, args.CandidateId, rf.currentTerm)
			rf.resetChan <- args.CandidateId
			}()
	}
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm{
		reply.Success = false
	}else{
		reply.Success = true
		if args.Term > rf.currentTerm{
			rf.currentTerm = args.Term
		}
		rf.voteFor = args.LeaderId
		rf.role = 0
		go func(){
			//fmt.Printf("Raft %v receive heartbeat from Raft %v at term %v\n", rf.me, args.LeaderId, args.Term)
			rf.resetChan <- args.LeaderId
		}()
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	_, isLeader = rf.GetState()

	if isLeader{
		rf.mu.Lock()
		index = len(rf.log) - 1
		entry := LogEntry{}
		entry.Term = rf.currentTerm
		entry.Command = command
		rf.log = append(rf.log, entry)
		term = rf.currentTerm
		rf.mu.Unlock()
	}

	//TODO: broadcast to followers

	return index, term, isLeader
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

	// Your initialization code here.
	rf.currentTerm	= 0
	rf.voteFor		= -1
	rf.log = make([]LogEntry, 1)
	rf.commitIndex	= 0
	rf.lastApplied	= 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.resetChan = make(chan int)
	rf.role = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func(){
		granted := 1
		stale := false
		grantChan := make(chan int)
		for{
			_, isleader := rf.GetState()
			//leader state
			if isleader{
				//fmt.Printf("Raft %v believes he is leader at term %v\n", rf.me, rf.currentTerm)
				select{
				case <- time.After(20 * time.Millisecond):
					_, isleader = rf.GetState()
					if isleader{
						for i := range rf.peers{
							if i == rf.me{
								continue
							}
							go func(ii int){
								rf.mu.Lock()
								//fmt.Printf("Raft %v send haertbeat to Raft %v at term %v\n", rf.me, ii, rf.currentTerm)
								appendEntriesArgs := AppendEntriesArgs{}
								appendEntriesArgs.Term = rf.currentTerm
								appendEntriesArgs.LeaderId = rf.me
								appendEntriesReply := &AppendEntriesReply{}
								rf.mu.Unlock()
								if rf.sendAppendEntries(ii, appendEntriesArgs, appendEntriesReply){
									rf.mu.Lock()
									if appendEntriesReply.Term > rf.currentTerm{
										rf.currentTerm = appendEntriesReply.Term
										rf.voteFor = -1
										rf.role = 0
									}
									rf.mu.Unlock()
								}
							}(i)
						}
					}
				}
				continue
			}
			//follower or candidate state
			ms := 150 + rand.Intn(150)
			//fmt.Printf("Raft %v sleep %v millisecond\n", rf.me, ms)
			select{
				case  <- rf.resetChan:
					//fmt.Printf("Raft %v reset by %v\n", rf.me, i)

				case grantTerm := <- grantChan:
					if grantTerm == rf.currentTerm{
						rf.mu.Lock()
						granted += 1
						rf.mu.Unlock()
						if !stale && granted >= (len(rf.peers)+1)/2{
							//fmt.Printf("Raft %v becomes leader for term %v with %v votes\n", rf.me, rf.currentTerm, granted)
							rf.mu.Lock()
							rf.voteFor = rf.me
							rf.role = 2
							rf.mu.Unlock()

							for i := range rf.peers{
								if i == rf.me{
									continue
								}
								go func(ii int){
									rf.mu.Lock()
									appendEntriesArgs := AppendEntriesArgs{}
									appendEntriesArgs.Term = rf.currentTerm
									appendEntriesArgs.LeaderId = rf.me
									appendEntriesReply := &AppendEntriesReply{}
									rf.mu.Unlock()
									if rf.sendAppendEntries(ii, appendEntriesArgs, appendEntriesReply){
										rf.mu.Lock()
										if appendEntriesReply.Term > rf.currentTerm{
											rf.currentTerm = appendEntriesReply.Term
											rf.voteFor = -1//failed the election
											rf.role = 0
										}
										rf.mu.Unlock()
									}
								}(i)
							}
						}
					}

				case <- time.After(time.Duration(ms) * time.Millisecond):
					//fmt.Printf("Raft %v timeout, %v millisecond\n", rf.me, ms)
					rf.mu.Lock()
					rf.role = 1
					rf.voteFor = rf.me
					rf.currentTerm += 1
					granted = 1
					stale = false
					rf.mu.Unlock()
					//fmt.Printf("Raft %v send request vote for term %v\n", rf.me, rf.currentTerm)
					for i := range rf.peers{
						if i == rf.me{
							continue
						}
						go func(ii int){
							rf.mu.Lock()
							request := RequestVoteArgs{}
							request.Term = rf.currentTerm
							request.CandidateId = rf.me
							reply := &RequestVoteReply{}
							rf.mu.Unlock()
							if rf.sendRequestVote(ii, request, reply){
								if reply.VoteGranted{
									go func(){
										grantChan <- request.Term
									}()
								}else{
									rf.mu.Lock()
									if reply.Term > rf.currentTerm{
										rf.currentTerm = reply.Term
										stale = true
									}
									rf.mu.Unlock()
								}
							}
						}(i)
					}
			}
		}
	}()

	return rf
}
