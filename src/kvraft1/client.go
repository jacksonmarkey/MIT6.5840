package kvraft

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

const RETRY_INTERVAL = 100

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	mu            sync.Mutex
	currentLeader int
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// You will have to modify this function.
	args := rpc.GetArgs{
		Key: key,
	}
	reply := rpc.GetReply{}
	ck.mu.Lock()
	lastKnownLeader := ck.currentLeader
	nextTryServer := lastKnownLeader
	ck.mu.Unlock()
	// fmt.Printf("(client)->[%v](server) sending Get%+v\n", nextTryServer, args)
	ok := ck.clnt.Call(ck.servers[nextTryServer], "KVServer.Get", &args, &reply)
	for !ok || reply.Err == rpc.ErrWrongLeader {
		// TODO: Should we create new args and reply objects in case an old server
		// with a pointer to the same args and reply overwrite those from the
		// most recent one?
		time.Sleep(RETRY_INTERVAL * time.Millisecond)
		ck.mu.Lock()
		// If another thread found a new leader and updated the Clerk, try that server
		// Otherwise, cycle through the next leader
		if ck.currentLeader != lastKnownLeader {
			lastKnownLeader = ck.currentLeader
			nextTryServer = lastKnownLeader
		} else {
			nextTryServer = (nextTryServer + 1) % len(ck.servers)
		}
		ck.mu.Unlock()
		// fmt.Printf("(client)->[%v](server) sending Get%+v\n", nextTryServer, args)
		ok = ck.clnt.Call(ck.servers[nextTryServer], "KVServer.Get", &args, &reply)
	}
	// fmt.Printf("(client)->[%v](server) success Get%+v\n", nextTryServer, reply)
	ck.mu.Lock()
	if ck.currentLeader != nextTryServer {
		ck.currentLeader = nextTryServer
	}
	ck.mu.Unlock()
	return reply.Value, reply.Version, reply.Err
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	reply := rpc.PutReply{}
	ck.mu.Lock()
	lastKnownLeader := ck.currentLeader
	nextTryServer := lastKnownLeader
	ck.mu.Unlock()
	// fmt.Printf("[%v].Put1, args=%+v\n", nextTryServer, args)
	ok := ck.clnt.Call(ck.servers[lastKnownLeader], "KVServer.Put", &args, &reply)
	// fmt.Printf("[%v].Put1, result=%+v\n", nextTryServer, reply)
	if ok && reply.Err != rpc.ErrWrongLeader {
		return reply.Err
	}
	for !ok || reply.Err == rpc.ErrWrongLeader {
		// fmt.Printf("(client)->[%v](server).Put got Reply%+v\n", nextTryServer, reply)
		time.Sleep(RETRY_INTERVAL * time.Millisecond)
		ck.mu.Lock()
		// If another thread found a new leader and updated the Clerk, try that server
		// Otherwise, cycle through the next leader
		if ck.currentLeader != lastKnownLeader {
			nextTryServer = ck.currentLeader
		} else {
			nextTryServer = (nextTryServer + 1) % len(ck.servers)
		}
		ck.mu.Unlock()
		// fmt.Printf("[%v].Putn, args=%+v\n", nextTryServer, args)
		ok = ck.clnt.Call(ck.servers[nextTryServer], "KVServer.Put", &args, &reply)
		// fmt.Printf("[%v].Putn, result=%+v\n", nextTryServer, reply)
	}
	ck.mu.Lock()
	if ck.currentLeader != nextTryServer {
		ck.currentLeader = nextTryServer
	}
	ck.mu.Unlock()
	if reply.Err == rpc.ErrVersion {
		return rpc.ErrMaybe
	}
	return reply.Err
}
