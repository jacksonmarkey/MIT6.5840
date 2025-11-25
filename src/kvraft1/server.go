package kvraft

import (
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type VersionedValue struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu    sync.Mutex
	store map[string]VersionedValue
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	// fmt.Printf("[%v].DoOp(%+v)\n", kv.me, req)
	switch args := req.(type) {
	case rpc.GetArgs:
		reply := rpc.GetReply{}
		kv.mu.Lock()
		v, ok := kv.store[args.Key]
		kv.mu.Unlock()
		if ok {
			reply.Value = v.value
			reply.Version = v.version
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
		return reply
	case rpc.PutArgs:
		// fmt.Println("PutArgs!")
		reply := rpc.PutReply{}
		kv.mu.Lock()
		defer kv.mu.Unlock()
		v, ok := kv.store[args.Key]
		// fmt.Printf("v=%+v, ok=%v\n", v, ok)
		switch {
		case !ok:
			if args.Version != 0 {
				reply.Err = rpc.ErrNoKey
				return reply
			}
			kv.store[args.Key] = VersionedValue{
				value:   args.Value,
				version: rpc.Tversion(1),
			}
			reply.Err = rpc.OK
		case v.version != args.Version:
			reply.Err = rpc.ErrVersion
		default:
			kv.store[args.Key] = VersionedValue{
				value:   args.Value,
				version: v.version + 1,
			}
			reply.Err = rpc.OK
		}
		return reply
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, submitResult := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	result, ok := submitResult.(rpc.GetReply)
	if ok {
		reply.Err = result.Err
		reply.Value = result.Value
		reply.Version = result.Version
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	// fmt.Printf("[%v].Put(%+v)\n", kv.me, args)
	err, submitResult := kv.rsm.Submit(*args)
	// fmt.Printf("err=%v, submitResult=%+v\n", err, submitResult)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	result, ok := submitResult.(rpc.PutReply)
	if ok {
		reply.Err = result.Err
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(rpc.ErrWrongLeader)
	labgob.Register(rpc.ErrMaybe)
	labgob.Register(rpc.ErrNoKey)
	labgob.Register(rpc.ErrVersion)

	kv := &KVServer{me: me}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	kv.store = make(map[string]VersionedValue)
	return []tester.IService{kv, kv.rsm.Raft()}
}
