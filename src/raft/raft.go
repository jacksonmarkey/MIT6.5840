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

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
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
	applyCh         chan ApplyMsg
}

type LogEntry struct {
	Command interface{}
	Term    int
}

const (
	Leader    = "leader"
	Follower  = "follower"
	Candidate = "candidate"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
	// Technically, if we're in the same term as the candidate sending us the RequestVote RPC, then either:
	// 1. We triggered our own term update by becoming a candidate, and we've already voted for ourselves
	// 2. We received a RequestVote RPC from another candidate of this term, updated our term, and already voted
	// 3. We received an AppendEntries RPC from the leader who already won the election for this term,
	// 		which whether or not we voted for them they won the election and this new candidate has no chance
	// 		and it doesn't matter if we give them the vote
	// Thus, if we are in the same term, we want to reject the vote
	// if args.Term == rf.currentTerm {
	// 	if rf.votedFor != -1 {
	// 		// we already voted this term, reject it
	// 		reply.VoteGranted = false
	// 	} else {
	// 		reply.VoteGranted = true
	// 		rf.votedFor = args.CandidateID
	// 		rf.lastHeartbeat = time.Now()
	// 	}
	// }
	if args.Term == rf.currentTerm && rf.votedFor != -1 {
		// we already voted this term, reject it
		reply.VoteGranted = false
		return
	}
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
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
	rf.currentTerm = args.Term
	rf.lastHeartbeat = time.Now()
	rf.state = Follower
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
	rf.currentTerm = electionTerm
	rf.state = Candidate
	rf.votedFor = rf.me
	voteCount := 1
	// prepare remaining RequestVote RPC args
	candidateID := rf.me
	lastLogIndex := len(rf.log) - 1
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
				// accidentally demote ourself to follower for another election
				// if this goroutine is stale
				if reply.Term > rf.currentTerm {
					rf.state = Follower
					rf.currentTerm = reply.Term
				}
				rf.mu.Unlock()
				return
			}
			if reply.VoteGranted {
				rf.mu.Lock()
				// check that we're stlil in this election's term
				if rf.currentTerm == electionTerm && rf.state == Candidate {
					voteCount += 1
					if voteCount > len(rf.peers)/2 {
						DPrintf("[%v] elected leader!", rf.me)
						rf.state = Leader
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						for i := 0; i < len(rf.nextIndex); i++ {
							rf.nextIndex[i] = len(rf.log)
							// rf.matchIndex[i] = 0
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
	index = len(rf.log)
	term = rf.currentTerm
	isLeader = (rf.state == Leader)
	if isLeader {
		DPrintf("[%v] added to log: {Index: %v, Term: %v}", rf.me, len(rf.log), term)
		newLogEntry := LogEntry{
			Command: command,
			Term:    term,
		}
		rf.log = append(rf.log, newLogEntry)
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		lastHeartbeat := rf.lastHeartbeat
		isleader := (rf.state == Leader)
		if !isleader && time.Since(lastHeartbeat) > rf.electionTimeOut {
			go rf.kickoffElection()
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applyMsgTicker() {
	for rf.killed() == false {
		rf.mu.Lock()
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
		}
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
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
	Term    int
	Success bool
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
	rf.state = Follower
	rf.currentTerm = leaderTerm
	// TODO: Move lock out of resetheartbeat function
	// so we can call it in here instead of directly resetting?
	rf.lastHeartbeat = time.Now()
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	if len(args.Entries) > 0 {
		DPrintf("[%v] added to log: {Index: %v, Term: %v}", rf.me, len(rf.log), args.Entries[0].Term)
	}
	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
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
				if rf.state != Leader || rf.currentTerm != startTerm {
					rf.mu.Unlock()
					break
				}
				nextIndex := rf.nextIndex[peerID]
				var entries []LogEntry
				if nextIndex < len(rf.log) {
					entries = rf.log[nextIndex:]
				}
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: nextIndex - 1,
					PrevLogTerm:  rf.log[nextIndex-1].Term,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(peerID, &args, &reply)
				rf.mu.Lock()
				// Don't know what happened while we waited for the RPC, so check this again
				if ok && rf.state == Leader && rf.currentTerm == startTerm {
					// check success and update accordingly
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.mu.Unlock()
						return
					}
					if !reply.Success {
						// TODO: decide whether to rate limit all AppendEntries RPCs
						// to 10RPC/sec, or if we can sendback and forth if they're being
						// rejected by followers, so that we find the right matchIndex faster
						rf.nextIndex[peerID] -= 1
						rf.mu.Unlock()
						continue
						// currently, this skips the time.Sleep() at the end of the loop
					}
					if reply.Success {
						rf.nextIndex[peerID] = len(rf.log)
						rf.matchIndex[peerID] = len(rf.log) - 1
					}
				}
				rf.mu.Unlock()
				// wait between sending rounds of heartbeats
				time.Sleep(150 * time.Millisecond)
			}
		}()
	}
}

func (rf *Raft) commitTicker(startTerm int) {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.currentTerm != startTerm || rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		for N := len(rf.log) - 1; N > rf.commitIndex; N -= 1 {
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
				if matchIdx >= N {
					count += 1
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				break
			}
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) resetElectionTimeOut() {
	min, max := 500, 750
	n := rand.Intn(max-min) + min
	rf.mu.Lock()
	rf.electionTimeOut = time.Duration(n) * time.Millisecond
	rf.lastHeartbeat = time.Now()
	rf.mu.Unlock()
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.applyCh = applyCh
	rf.state = Follower
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{
		Term:    0,
		Command: struct{}{},
	}
	rf.resetElectionTimeOut()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start goroutine to send apply messages
	go rf.applyMsgTicker()

	return rf
}
