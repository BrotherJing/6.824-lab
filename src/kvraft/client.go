package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"

//import "fmt"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader	int
	id			int64
	reqid		int
	mu			sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = nrand()
	ck.reqid = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{Key: key}
	args.Id = ck.id
	ck.mu.Lock()
	args.ReqId = ck.reqid
	ck.reqid++
	ck.mu.Unlock()
	tryone := func (i int) (string, bool) {
		reply := &GetReply{}
		ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
		if ok{
			if reply.Err != ""{
				if !reply.WrongLeader{
					//fmt.Printf("%v\n", reply.Err)
				}
			}else{
				ck.lastLeader = i
				return reply.Value, true
			}
		}else{
			//fmt.Printf("%v dropped\n", args)
		}
		return "", false
	}
	if res, ok:=tryone(ck.lastLeader); ok{
		return res
	}
	for{
		for i := range ck.servers{
			if res, ok:=tryone(i); ok{
				return res
			}
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, Op: op}
	args.Id = ck.id
	ck.mu.Lock()
	args.ReqId = ck.reqid
	ck.reqid++
	ck.mu.Unlock()
	tryone := func (i int) bool{
		reply := &PutAppendReply{}
		ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
		if ok{
			if reply.Err == ""{
				ck.lastLeader = i
				return true
			}else{
				if !reply.WrongLeader{
					//fmt.Printf("%v\n", reply.Err)
				}
			}
		}else{
			//fmt.Printf("%v dropped\n", args)
		}
		return false
	}
	if tryone(ck.lastLeader){
		return
	}
	for{
		for i := range ck.servers{
			if tryone(i){
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
