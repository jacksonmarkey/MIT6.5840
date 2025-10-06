package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type VersionedValue struct {
	value   string
	version rpc.Tversion
}
type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	store map[string]VersionedValue
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.store = make(map[string]VersionedValue)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v, ok := kv.store[args.Key]
	if ok {
		reply.Value = v.value
		reply.Version = v.version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}

}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v, ok := kv.store[args.Key]
	switch {
	case !ok:
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
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

}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
