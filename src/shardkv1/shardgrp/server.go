package shardgrp

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type VersionedValue struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your code here
	mu    sync.Mutex
	store map[string]VersionedValue
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch args := req.(type) {
	case rpc.GetArgs:
		reply := rpc.GetReply{}
		kv.mu.Lock()
		v, ok := kv.store[args.Key]
		kv.mu.Unlock()
		if ok {
			reply.Value = v.Value
			reply.Version = v.Version
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
				Value:   args.Value,
				Version: rpc.Tversion(1),
			}
			reply.Err = rpc.OK
		case v.Version != args.Version:
			reply.Err = rpc.ErrVersion
		default:
			kv.store[args.Key] = VersionedValue{
				Value:   args.Value,
				Version: v.Version + 1,
			}
			reply.Err = rpc.OK
		}
		return reply
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	if e.Encode(kv.store) != nil {
		fmt.Println("Encoding error!")
	}
	kv.mu.Unlock()
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var store map[string]VersionedValue
	if d.Decode(&store) != nil {
		fmt.Println("Decoding error!")
	}
	kv.mu.Lock()
	kv.store = store
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
	// fmt.Printf("Submitting... %v\n", args)
	err, submitResult := kv.rsm.Submit(*args)
	// fmt.Printf("Submitted... %v\n", args)
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
	// Your code here
	// fmt.Printf("Submitting... %v\n", args)
	err, submitResult := kv.rsm.Submit(*args)
	// fmt.Printf("Submitted... %v\n", args)
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

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
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

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here
	kv.store = make(map[string]VersionedValue)
	return []tester.IService{kv, kv.rsm.Raft()}
}
