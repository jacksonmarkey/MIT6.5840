package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	SUBMIT_TIMEOUT     = 2000
	SNAPSHOT_THRESHOLD = 0.75
	SNAPSHOT_INTERVAL  = 50
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  int
	Req any
}

type OpResult struct {
	op     Op
	result any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	idCounter   int
	doOpResults map[int]chan OpResult
	lastApplied int
	killed      bool
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		doOpResults:  make(map[int]chan OpResult),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	snap := persister.ReadSnapshot()
	if len(snap) > 0 {
		rsm.sm.Restore(snap)
	}
	go rsm.reader()
	go rsm.persistTicker()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) persistTicker() {
	if rsm.maxraftstate == -1 {
		return
	}
	for rsm.killed == false {
		// fmt.Printf("[%v] RSM (size(%v) > thresh(%v))\n", rsm.me, rsm.Raft().PersistBytes(), int(float64(rsm.maxraftstate)*SNAPSHOT_THRESHOLD))
		if rsm.Raft().PersistBytes() > int(float64(rsm.maxraftstate)*SNAPSHOT_THRESHOLD) {
			// fmt.Printf("[%v] RSM trimming (size(%v) > thresh(%v))\n", rsm.me, rsm.Raft().PersistBytes(), int(float64(rsm.maxraftstate)*SNAPSHOT_THRESHOLD))
			rsm.mu.Lock()
			lastApplied := rsm.lastApplied
			snap := rsm.sm.Snapshot()
			rsm.mu.Unlock()
			rsm.Raft().Snapshot(lastApplied, snap)
		}
		time.Sleep(SNAPSHOT_INTERVAL * time.Millisecond)
	}
}

func (rsm *RSM) reader() {
	// fmt.Println("Reader")
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)
			rsm.mu.Lock()
			if msg.CommandIndex <= rsm.lastApplied {
				continue
			}
			rsm.lastApplied = msg.CommandIndex
			result := rsm.sm.DoOp(op.Req)
			opResult := OpResult{
				op:     op,
				result: result,
			}
			resultCh, ok := rsm.doOpResults[msg.CommandIndex]
			if ok {
				resultCh <- opResult
				delete(rsm.doOpResults, msg.CommandIndex)
			}
			rsm.mu.Unlock()
		}
		if msg.SnapshotValid {
			raft.DPrintf("[%v]RSM -< applyMsg %+v\n", rsm.me, msg)
			rsm.mu.Lock()
			if msg.SnapshotIndex <= rsm.lastApplied {
				rsm.mu.Unlock()
				continue
			}
			rsm.sm.Restore(msg.Snapshot)
			rsm.mu.Unlock()
		}
		// fmt.Println("Done")
	}
	rsm.killed = true
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	// fmt.Println("Submit")
	_, isLeader := rsm.rf.GetState()
	if !isLeader {
		// fmt.Println("NotLeader")
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}
	rsm.mu.Lock()
	id := rsm.idCounter
	rsm.idCounter += 1
	rsm.mu.Unlock()
	op := Op{Me: rsm.me, Id: id, Req: req}
	logIndex, startTerm, isLeader := rsm.rf.Start(op)
	// fmt.Printf("[%v] Starting %+v, logIndex=%v, startTerm=%v, isLeader=%v\n", rsm.me, op, logIndex, startTerm, isLeader)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}
	resultCh := make(chan OpResult)
	rsm.mu.Lock()
	rsm.doOpResults[logIndex] = resultCh
	rsm.mu.Unlock()
	select {
	case opResult := <-resultCh:
		currentTerm, isLeader := rsm.rf.GetState()
		if !isLeader || currentTerm != startTerm {
			return rpc.ErrWrongLeader, nil
		}
		if opResult.op == op {
			return rpc.OK, opResult.result
		}
	case <-time.After(SUBMIT_TIMEOUT * time.Millisecond):
		rsm.mu.Lock()
		defer rsm.mu.Unlock()
		staleCh, ok := rsm.doOpResults[logIndex]
		if ok && staleCh == resultCh {
			delete(rsm.doOpResults, logIndex)
		}
		currentTerm, isLeader := rsm.rf.GetState()
		if !isLeader || currentTerm != startTerm || rsm.lastApplied >= logIndex {
			return rpc.ErrWrongLeader, nil
		}
	}

	// TODO: what to return here?
	return rpc.ErrWrongLeader, nil
}
