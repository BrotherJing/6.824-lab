package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	//"fmt"
	"time"
	"bytes"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op		string
	Key		string
	Value	string
	ReqId	int
	Id		int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	replyCh	map[int]chan Op
	db		map[string]string
	reqids	map[int64]int
}


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cmd := Op{Op:"Get", Key:args.Key, Id:args.Id, ReqId:args.ReqId}
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader{
		reply.WrongLeader = true
		reply.Err = "Not a leader."
		return
	}
	//fmt.Printf("%v\n", args)
	reply.WrongLeader = false
	ch, ok := kv.replyCh[index]
	if !ok{
		kv.replyCh[index] = make(chan Op)
		ch = kv.replyCh[index]
	}
	select{
	case cmdApplied := <- ch:
		if cmd != cmdApplied{
			reply.Err = "a different request has appeared at the index returned by Start()"
			return
		}
		kv.mu.Lock()
		reply.Value = kv.db[args.Key]
		kv.mu.Unlock()
	case <- time.After(1000 * time.Millisecond):
		reply.Err = "Timeout."
		return
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	cmd := Op{Op:args.Op, Key:args.Key, Value:args.Value, Id:args.Id, ReqId:args.ReqId}
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader{
		reply.WrongLeader = true
		reply.Err = "Not a leader."
		return
	}
	//fmt.Printf("%v\n", args)
	reply.WrongLeader = false
	ch, ok := kv.replyCh[index]
	if !ok{
		kv.replyCh[index] = make(chan Op)
		ch = kv.replyCh[index]
	}
	select{
	case cmdApplied := <- ch:
		if cmd != cmdApplied{
			reply.Err = "a different request has appeared at the index returned by Start()"
			return
		}
	case <- time.After(1000 * time.Millisecond):
		reply.Err = "Timeout."
		return
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	kv.replyCh = make(map[int]chan Op)
	kv.db = make(map[string]string)
	kv.reqids = make(map[int64]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go func() {
		for m := range kv.applyCh {
			if m.UseSnapshot {
				// ignore the snapshot
				kv.mu.Lock()
				var temp int
				r := bytes.NewBuffer(m.Snapshot)
				d := gob.NewDecoder(r)
				d.Decode(&temp)
				d.Decode(&temp)
				kv.db = make(map[string]string)
				kv.reqids = make(map[int64]int)
				d.Decode(&kv.db)
				d.Decode(&kv.reqids)
				kv.mu.Unlock()
			} else if v, ok := (m.Command).(Op); ok {
				//fmt.Printf("Raft %v committed %v at index %v\n", i, v, m.Index)
				kv.mu.Lock()
				reqid, ok := kv.reqids[v.Id]
				if ok && v.ReqId <= reqid{
					//duplicated!
				}else{
					kv.reqids[v.Id] = v.ReqId
					switch v.Op{
					case "Get":
					case "Put":
						kv.db[v.Key] = v.Value
					case "Append":
						kv.db[v.Key] += v.Value
					}
				}
				ch, ok := kv.replyCh[m.Index]
				if !ok{
					kv.replyCh[m.Index] = make(chan Op)
				}else{
					select{
					case <- ch:
					default:
					}
					go func(){
						ch <- v
					}()
				}
				if maxraftstate != -1 && kv.rf.GetPersistSize() > maxraftstate{
					w := new(bytes.Buffer)
					e := gob.NewEncoder(w)
					e.Encode(kv.db)
					e.Encode(kv.reqids)
					data := w.Bytes()
					go kv.rf.StartSnapshot(data, m.Index)
				}
				kv.mu.Unlock()
			}
		}
	}()

	return kv
}
