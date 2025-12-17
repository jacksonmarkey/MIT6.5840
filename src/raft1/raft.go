package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	LEADER                = "leader"
	FOLLOWER              = "follower"
	CANDIDATE             = "candidate"
	COMMIT_INTERVAL       = 10
	HEARTBEAT_INTERVAL    = 35
	MIN_ELECTION_INTERVAL = 400
	MAX_ELECTION_INTERVAL = 500
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm     int
	votedFor        int // -1 if hasn't voted
	log             []LogEntry
	lastHeartbeat   time.Time
	electionTimeOut time.Duration
	state           string
	commitIndex     int
	lastApplied     int
	nextIndex       []int
	matchIndex      []int
	applyCh         chan raftapi.ApplyMsg
	notifyApplyCh   chan struct{}

	snapshot           []byte
	lastSnapshotIndex  int
	lastSnapshotTerm   int
	hasSnapshotToApply bool
}

type LogEntry struct {
	Command interface{}
	Term    int
}

func init() {
	// DPrintf("initializing...")
	labgob.Register(LogEntry{})
	labgob.Register(int(0))
	labgob.Register(struct{}{})
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log) != nil ||
		e.Encode(rf.lastSnapshotIndex) != nil ||
		e.Encode(rf.lastSnapshotTerm) != nil {
		fmt.Println("Encoding error!")
	}
	rf.persister.Save(w.Bytes(), rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
	var lastSnapshotIndex int
	var lastSnapshotTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastSnapshotIndex) != nil ||
		d.Decode(&lastSnapshotTerm) != nil {
		fmt.Println("Decoding error!")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.lastSnapshotIndex = lastSnapshotIndex
	rf.lastSnapshotTerm = lastSnapshotTerm
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// ignore stale or redundant snapshots
	DPrintf("[%v] taking snapshot at index=%v", rf.me, index)
	if index <= rf.lastSnapshotIndex {
		return
	}
	indexDelta := index - rf.lastSnapshotIndex
	// Note that we retain the log entry at lastSnapshotIndex,
	// in keeping with the convention of our initial log entry
	// {Term: 0, command: struct{}{}},
	// to ensure that followers always have at least one committed log entry
	DPrintf("indexDelta = %v", indexDelta)
	rf.log = rf.log[indexDelta:]
	rf.lastSnapshotIndex = index
	rf.lastSnapshotTerm = rf.log[0].Term
	rf.snapshot = snapshot
	rf.persist()
	DPrintf("[%v] successful snapshot, len(log)=%v", rf.me, len(rf.log))
}

type ApplySnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type ApplySnapshotReply struct {
	Term int
}

func (rf *Raft) ApplySnapshot(args *ApplySnapshotArgs, reply *ApplySnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%v]->[%v] applying snapshot... args=%+v", args.LeaderID, rf.me, args)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm || rf.state != FOLLOWER {
		rf.convertToFollowerLocked(args.Term)
	}
	if args.LastIncludedIndex <= rf.commitIndex {
		rf.resetElectionTimeOutLocked()
		return
	}
	i := args.LastIncludedIndex - rf.lastSnapshotIndex
	if i >= 0 && i < len(rf.log) && rf.log[i].Term == args.LastIncludedTerm {
		rf.resetElectionTimeOutLocked()
		return
	}
	rf.snapshot = args.Data
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: args.LastIncludedTerm, Command: struct{}{}}
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.lastSnapshotIndex = args.LastIncludedIndex
	rf.lastSnapshotTerm = args.LastIncludedTerm
	rf.hasSnapshotToApply = true
	rf.notifyApplier()
	DPrintf("[%v] Snapshot apply queued: now commitIndex=%v, lastApplied=%v, len_log=%v", rf.me, rf.commitIndex, rf.lastApplied, len(rf.log))
	rf.persist()
	rf.resetElectionTimeOutLocked()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		// request is stale, reject it
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		// we already voted this term, reject it
		reply.VoteGranted = false
		return
	}
	// Update our server term, even if we withhold our vote due to log term restrictions
	if args.Term > rf.currentTerm {
		rf.convertToFollowerLocked(args.Term)
	}

	lastLogIndex := len(rf.log) - 1 + rf.lastSnapshotIndex
	lastLogTerm := rf.log[len(rf.log)-1].Term
	// reject if our log is more up-to-date
	if lastLogTerm > args.LastLogTerm {
		reply.VoteGranted = false
		return
	}
	if lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex {
		reply.VoteGranted = false
		return
	}
	// If none of the above executed, then we give our vote
	reply.VoteGranted = true
	rf.votedFor = args.CandidateID
	rf.persist()
	rf.resetElectionTimeOutLocked()
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) kickoffElection() {
	rf.mu.Lock()
	// increment term, change to candidate, and vote for self
	electionTerm := rf.currentTerm + 1
	DPrintf("[%v].term(%v)->(%v), starting election", rf.me, rf.currentTerm, electionTerm)
	rf.currentTerm = electionTerm
	rf.state = CANDIDATE
	rf.votedFor = rf.me
	rf.persist()
	voteCount := 1
	// prepare remaining RequestVote RPC args
	candidateID := rf.me
	lastLogIndex := len(rf.log) - 1 + rf.lastSnapshotIndex
	lastLogTerm := rf.log[len(rf.log)-1].Term
	rf.mu.Unlock()
	for peerID := range rf.peers {
		if peerID == rf.me {
			continue
		}
		args := RequestVoteArgs{
			Term:         electionTerm,
			CandidateID:  candidateID,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		go func() {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peerID, &args, &reply)
			if !ok {
				// If we don't get any reply, no need to retry this peer's RPC.
				// We will either win this election without its vote, start a new election next term,
				// or recognize another leader.
				return
			}
			if reply.Term > electionTerm {
				// quit election
				rf.mu.Lock()
				// We need this second check in case we've already incremented our term
				// and are a candidate or leader for a later term. We don't want to
				// accidentally demote ourself to follower in a future term election
				if reply.Term > rf.currentTerm {
					rf.convertToFollowerLocked(reply.Term)
				}
				rf.mu.Unlock()
				return
			}
			if reply.VoteGranted {
				rf.mu.Lock()
				// check that we're stlil in this election's term
				if rf.currentTerm == electionTerm && rf.state == CANDIDATE {
					voteCount += 1
					if voteCount > len(rf.peers)/2 {
						DPrintf("(Term:%v)[%v] elected leader!", rf.currentTerm, rf.me)
						rf.state = LEADER
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						for i := 0; i < len(rf.nextIndex); i++ {
							rf.nextIndex[i] = len(rf.log) + rf.lastSnapshotIndex
							rf.matchIndex[i] = 0
						}
						go rf.lead(rf.currentTerm)
					}
				}
				rf.mu.Unlock()
			}
		}()
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	rf.mu.Lock()

	index = len(rf.log) + rf.lastSnapshotIndex
	term = rf.currentTerm
	isLeader = (rf.state == LEADER)
	if isLeader {
		newLogEntry := LogEntry{
			Command: command,
			Term:    term,
		}
		rf.log = append(rf.log, newLogEntry)
		rf.persist()
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	// fmt.Printf("[%v] Killed", rf.me)
	// close(rf.applyCh)
	close(rf.notifyApplyCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		rf.mu.Lock()
		lastHeartbeat := rf.lastHeartbeat
		isleader := (rf.state == LEADER)
		if !isleader && time.Since(lastHeartbeat) > rf.electionTimeOut {
			rf.resetElectionTimeOutLocked()
			go rf.kickoffElection()
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) notifyApplier() {
	if rf.killed() {
		return
	}
	select {
	case rf.notifyApplyCh <- struct{}{}:
		// Successfully sent notification
	default:
		// Channel already has a pending notification, skip
	}
}

func (rf *Raft) applyMsgTicker() {
	defer close(rf.applyCh)
	for rf.killed() == false {
		select {
		case <-rf.notifyApplyCh:
			// Something to apply, continue below
		case <-time.After(10 * time.Millisecond):
			// Periodic check in case we missed a notification
		}
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		var msgQueue = make([]raftapi.ApplyMsg, 0)
		if rf.hasSnapshotToApply {
			DPrintf("[%v] Applying snapshot...", rf.me)
			msgQueue = append(msgQueue, raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.lastSnapshotTerm,
				SnapshotIndex: rf.lastSnapshotIndex,
			})
		}
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i-rf.lastSnapshotIndex].Command,
				CommandIndex: i,
			}
			msgQueue = append(msgQueue, msg)
		}
		rf.hasSnapshotToApply = false
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()
		for _, msg := range msgQueue {
			if rf.killed() {
				return
			}
			rf.applyCh <- msg
			// if msg.SnapshotValid {
			// 	DPrintf("[%v] Applied snapshot!", rf.me)
			// }
		}
	}
}

// field names must start with capital letters!
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// field names must start with capital letters!
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	leaderTerm := args.Term
	if leaderTerm < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.resetElectionTimeOutLocked()
	if leaderTerm > rf.currentTerm || rf.state != FOLLOWER {
		rf.convertToFollowerLocked(leaderTerm)
	}
	if args.PrevLogIndex >= len(rf.log)+rf.lastSnapshotIndex ||
		args.PrevLogIndex < rf.lastSnapshotIndex {
		reply.ConflictTerm = -1
		reply.ConflictIndex = len(rf.log) + rf.lastSnapshotIndex
		reply.Success = false
		return
	}
	if rf.log[args.PrevLogIndex-rf.lastSnapshotIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex-rf.lastSnapshotIndex].Term
		i := args.PrevLogIndex - 1 - rf.lastSnapshotIndex
		for i >= 0 {
			if rf.log[i].Term != reply.ConflictTerm {
				break
			}
			i--
		}
		reply.ConflictIndex = i + 1 + rf.lastSnapshotIndex
		reply.Success = false
		return
	}
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i - rf.lastSnapshotIndex
		if index >= len(rf.log) || rf.log[index].Term != entry.Term {
			rf.log = rf.log[:index]
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
		rf.commitIndex = args.LeaderCommit
		if lastNewEntryIndex < args.LeaderCommit {
			rf.commitIndex = lastNewEntryIndex
		}
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) lead(startTerm int) {
	go rf.commitTicker(startTerm)
	for peerID := range rf.peers {
		if peerID == rf.me {
			continue
		}
		go func() {
			for rf.killed() == false {
				rf.mu.Lock()
				if rf.state != LEADER || rf.currentTerm != startTerm {
					rf.mu.Unlock()
					break
				}
				nextIndex := rf.nextIndex[peerID]
				if nextIndex <= rf.lastSnapshotIndex {
					args := ApplySnapshotArgs{
						Term:              rf.currentTerm,
						LeaderID:          rf.me,
						LastIncludedIndex: rf.lastSnapshotIndex,
						LastIncludedTerm:  rf.lastSnapshotTerm,
						Data:              rf.snapshot,
					}
					reply := ApplySnapshotReply{}
					go func() {
						// DPrintf("[%v]->[%v] ApplySnapshotRPC, nextIndex=%v, lastSnapshotIndex=%v", rf.me, peerID, nextIndex, rf.lastSnapshotIndex)
						ok := rf.peers[peerID].Call("Raft.ApplySnapshot", &args, &reply)
						// DPrintf("[%v]->[%v] complete, reply=%+v", rf.me, peerID, reply)
						if ok {
							rf.mu.Lock()
							if startTerm < reply.Term {
								if rf.currentTerm < reply.Term {
									rf.convertToFollowerLocked(reply.Term)
								}
								rf.mu.Unlock()
								return
							}
							rf.matchIndex[peerID] = args.LastIncludedIndex
							rf.nextIndex[peerID] = rf.lastSnapshotIndex + len(rf.log)
							rf.mu.Unlock()
						}
					}()
					rf.mu.Unlock()
					time.Sleep(HEARTBEAT_INTERVAL * time.Millisecond)
					continue
				}
				var entries []LogEntry
				if nextIndex < len(rf.log)+rf.lastSnapshotIndex {
					entries = rf.log[nextIndex-rf.lastSnapshotIndex:]
				}
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: nextIndex - 1,
					PrevLogTerm:  rf.log[nextIndex-1-rf.lastSnapshotIndex].Term,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()
				go func() {
					// start := time.Now()
					reply := AppendEntriesReply{}
					ok := rf.sendAppendEntries(peerID, &args, &reply)
					// DPrintf("[%v]->[%v] AppendEntries.Success=%v, took %vms", rf.me, peerID, reply.Success, time.Since(start).Milliseconds())
					rf.mu.Lock()
					// DPrintf("[%v] lead()2 Acquired Lock", rf.me)
					defer rf.mu.Unlock()
					// Don't know what happened while we waited for the RPC, so check this again
					if ok && rf.state == LEADER && rf.currentTerm == startTerm {
						// check success and update accordingly
						if reply.Term > rf.currentTerm {
							rf.convertToFollowerLocked(reply.Term)
							return
						}
						// check if unordered stale reply from a retry after a hanging RPC call
						if args.PrevLogIndex != rf.nextIndex[peerID]-1 {
							return
						}
						if !reply.Success {
							if reply.ConflictTerm == -1 {
								rf.nextIndex[peerID] = reply.ConflictIndex
								return
							}
							for i := len(rf.log) - 1; i >= 0; i-- {
								if rf.log[i].Term == reply.ConflictTerm {
									rf.nextIndex[peerID] = i + 1 + rf.lastSnapshotIndex
									break
								}
								if rf.log[i].Term < reply.ConflictTerm {
									rf.nextIndex[peerID] = reply.ConflictIndex
									break
								}
							}
							return
						}
						// otherwise, success
						rf.nextIndex[peerID] = len(rf.log) + rf.lastSnapshotIndex
						rf.matchIndex[peerID] = args.PrevLogIndex + len(args.Entries)
					}
				}()
				// wait between sending rounds of heartbeats
				time.Sleep(HEARTBEAT_INTERVAL * time.Millisecond)
			}
		}()
	}
}

func (rf *Raft) commitTicker(startTerm int) {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.currentTerm != startTerm || rf.state != LEADER {
			rf.mu.Unlock()
			return
		}
		for N := len(rf.log) - 1; N > rf.commitIndex-rf.lastSnapshotIndex; N -= 1 {
			if rf.log[N].Term != rf.currentTerm {
				break
				// can actually call break rather than continue,
				// because log.Term is monotonically increasing over the log
				// with max term the current term of the leader
			}
			// TODO: better algorithm for checking the existance of N using quicksort or quickselect?
			count := 1 // we already have the log entry
			for peerID, matchIdx := range rf.matchIndex {
				if peerID == rf.me {
					continue
				}
				if matchIdx >= N+rf.lastSnapshotIndex {
					count += 1
				}
			}
			if count > len(rf.peers)/2 {
				DPrintf("(Term:%v)[%v] set commitIndex (%v)->(%v)", rf.currentTerm, rf.me, rf.commitIndex, N+rf.lastSnapshotIndex)
				rf.commitIndex = N + rf.lastSnapshotIndex
				rf.notifyApplier()
				break
			}
		}
		rf.mu.Unlock()
		time.Sleep(COMMIT_INTERVAL * time.Millisecond)
	}
}

// only call this function if we already have the lock!!!
func (rf *Raft) resetElectionTimeOutLocked() {
	n := MIN_ELECTION_INTERVAL + rand.Intn(MAX_ELECTION_INTERVAL-MIN_ELECTION_INTERVAL)
	rf.electionTimeOut = time.Duration(n) * time.Millisecond
	rf.lastHeartbeat = time.Now()
}

// only call this function if we already have the lock!!!
func (rf *Raft) convertToFollowerLocked(term int) {
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.persist()
	rf.resetElectionTimeOutLocked()
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.applyCh = applyCh
	rf.state = FOLLOWER
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{
		Term:    0,
		Command: struct{}{},
	}
	rf.notifyApplyCh = make(chan struct{}, 1)
	// initialize from state persisted before a crash
	if rf.persister.RaftStateSize() > 0 {
		rf.readPersist(persister.ReadRaftState())
		DPrintf("[%v] persisted from state", rf.me)
		DPrintf("[%v]={len(log)=%v, lastSnapshotIndex=%v, ...}", rf.me, len(rf.log), rf.lastSnapshotIndex)
	}

	if persister.SnapshotSize() > 0 {
		DPrintf("Reading snapshot...")
		rf.snapshot = persister.ReadSnapshot()
		rf.lastApplied = rf.lastSnapshotIndex
		rf.commitIndex = rf.lastSnapshotIndex
		rf.hasSnapshotToApply = true
		rf.notifyApplier()
		DPrintf("[%v] Snapshot apply queued, commitIndex=%v, lastApplied=%v, len_log=%v", rf.me, rf.commitIndex, rf.lastApplied, len(rf.log))
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	// start goroutine to send apply messages
	go rf.applyMsgTicker()

	// reset election timeout
	rf.mu.Lock()
	rf.resetElectionTimeOutLocked()
	rf.mu.Unlock()

	return rf
}
