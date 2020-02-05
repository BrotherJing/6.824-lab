package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Type int

const (
	PutOp    Type = 0
	AppendOp      = 1
	GetOp         = 2
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType Type
	Key    string
	Value  string
	Id     int64
	Seq    int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store   map[string]string
	replyCh map[int]chan int
	lastSeq map[int64]int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{}
	op.Id = args.Id
	op.Seq = args.Seq
	op.OpType = GetOp
	op.Key = args.Key
	index, term, ok := kv.rf.Start(op)
	if !ok {
		reply.WrongLeader = true
		return
	}
	kv.mu.Lock()
	ch, ok := kv.replyCh[index]
	if !ok {
		kv.replyCh[index] = make(chan int)
		ch = kv.replyCh[index]
	}
	kv.mu.Unlock()

	select {
	case currTerm := <-ch:
		// leader change after call rf.Start() but before commit
		if term != currTerm {
			reply.WrongLeader = true
			return
		}
	case <-time.After(1000 * time.Millisecond):
		reply.WrongLeader = true // just to tell client to retry
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, ok := kv.store[args.Key]; ok {
		reply.Value = v
	} else {
		reply.Err = ErrNoKey
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{}
	op.Id = args.Id
	op.Seq = args.Seq
	if args.Op == "Put" {
		op.OpType = PutOp
	} else {
		op.OpType = AppendOp
	}
	op.Key = args.Key
	op.Value = args.Value
	index, term, ok := kv.rf.Start(op)
	if !ok {
		reply.WrongLeader = true
		return
	}
	kv.mu.Lock()
	ch, ok := kv.replyCh[index]
	if !ok {
		kv.replyCh[index] = make(chan int)
		ch = kv.replyCh[index]
	}
	kv.mu.Unlock()

	select {
	case currTerm := <-ch:
		// leader change after call rf.Start() but before commit
		if term != currTerm {
			reply.WrongLeader = true
			return
		}
	case <-time.After(1000 * time.Millisecond):
		reply.WrongLeader = true // just to tell client to retry
		return
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.replyCh = make(map[int]chan int)
	kv.lastSeq = make(map[int64]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go func() {
		for m := range kv.applyCh {
			if m.CommandValid == false {
				// ignored
			} else if op, ok := (m.Command).(Op); ok {
				kv.mu.Lock()
				duplicated := false
				if seq, ok := kv.lastSeq[op.Id]; ok {
					duplicated = op.Seq <= seq
				}
				// fmt.Printf("server %v commit %v\n", kv.me, op.Value)
				if !duplicated {
					kv.lastSeq[op.Id] = op.Seq
					switch op.OpType {
					case AppendOp:
						if v, ok := kv.store[op.Key]; ok {
							kv.store[op.Key] = v + op.Value
							break
						}
					case PutOp:
						kv.store[op.Key] = op.Value
						break
					default:
						break
					}
				}
				ch, ok := kv.replyCh[m.CommandIndex]
				if ok {
					go func() {
						term, _ := kv.rf.GetState()
						ch <- term
					}()
				}
				kv.mu.Unlock()
			}
		}
	}()

	return kv
}
