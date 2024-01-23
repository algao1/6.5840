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
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type StateType int

const (
	Follower StateType = iota
	Candidate
	Leader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[st]
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm        int
	currentState       StateType
	votedFor           int
	tickFunc           func()
	lastHeartbeatEvent time.Time
	lastElectionEvent  time.Time

	commitIndex        int // index of highest log entry known to be committed.
	lastApplied        int // index of highest log entry applied to state machine.
	nextIndex          []int
	matchIndex         []int
	newCommitReadyChan chan struct{}
	applyCh            chan ApplyMsg
	log                []LogEntry
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.currentState == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.dlog("AppendEntries: %+v, currentTerm=%v", args, rf.currentTerm)
	if args.Term > rf.currentTerm {
		rf.dlog("... term out of date in AppendEntries")
		rf.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == rf.currentTerm {
		if rf.currentState != Follower {
			rf.becomeFollower(args.Term)
		}
		rf.lastElectionEvent = time.Now()

		// Does our log contain an entry at PrevLogIndex whose term
		// matches PrevLogTerm? Note that in the extreme case of PrevLogIndex = 0
		// this is vacuously true.
		if args.PrevLogIndex == 0 ||
			(args.PrevLogIndex < len(rf.log) && args.PrevLogTerm == rf.log[args.PrevLogIndex].Term) {
			reply.Success = true

			// Find an insertion point -- where there's a term mismatch beween
			// the existinglog starting at PrevLogIndex + 1 and the new entries
			// sent in the RPC.
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for {
				if logInsertIndex >= len(rf.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if rf.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}

			// At the end of this loop:
			// - logInsertIndex points at the end of the log, or an index
			//   where the term mismatches with an entry from the leader
			// - newEntriesIndex points at the end of Entries, or an index
			//   where the term mismatches with the corresponding log entry
			if newEntriesIndex < len(args.Entries) {
				rf.dlog("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
				rf.log = append(rf.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				rf.dlog("... log is now: %v", rf.log)
				rf.persist()
			}

			// Set commit index.
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
				rf.dlog("... setting commitIndex=%d", rf.commitIndex)
				rf.newCommitReadyChan <- struct{}{}
			}
		} else if args.PrevLogIndex >= len(rf.log) {
			reply.ConflictIndex = len(rf.log)
			reply.ConflictTerm = 0
		} else {
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
			var i int
			for i = args.PrevLogIndex - 1; i >= 0; i-- {
				if rf.log[i].Term != reply.ConflictTerm {
					break
				}
			}
			reply.ConflictIndex = i + 1
		}
	}
	reply.Term = rf.currentTerm
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) lastLogIndexAndTerm() (int, int) {
	lastLogIndex := len(rf.log) - 1
	return lastLogIndex, rf.log[lastLogIndex].Term
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLogIndex, lastLogTerm := rf.lastLogIndexAndTerm()
	rf.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, rf.currentTerm, rf.votedFor)

	if args.Term > rf.currentTerm {
		rf.dlog("... term out of date in RequestVote")
		rf.becomeFollower(args.Term)
	}

	if args.Term == rf.currentTerm &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastElectionEvent = time.Now()
		rf.persist()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
	rf.dlog("... RequestVote reply: %+v", reply)
}

func (rf *Raft) becomeFollower(newTerm int) {
	rf.currentTerm = newTerm
	rf.currentState = Follower
	rf.votedFor = -1
	rf.lastElectionEvent = time.Now()
	rf.tickFunc = rf.tickFollower
	rf.persist()
}

func (rf *Raft) tickFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if time.Since(rf.lastElectionEvent) > rf.electionTimeout() {
		rf.dlog("time exceeded for last election event, becoming candidate")
		rf.becomeCandidate()
	}
}

func (rf *Raft) becomeCandidate() {
	rf.currentTerm = rf.currentTerm + 1
	rf.currentState = Candidate
	rf.votedFor = rf.me
	rf.tickFunc = rf.tickCandidate
	rf.persist()
}

func (rf *Raft) tickCandidate() {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	if time.Since(rf.lastElectionEvent) <= rf.electionTimeout() {
		rf.mu.Unlock()
		return
	}
	rf.dlog("time elapsed since last election event %d", time.Since(rf.lastElectionEvent).Milliseconds())
	rf.currentTerm++
	votes := 1
	savedCurrentTerm := rf.currentTerm
	rf.lastElectionEvent = time.Now()
	rf.mu.Unlock()

	// Send RequestVote RPCs to all other servers concurrently.
	for peerId := range rf.peers {
		if peerId == rf.me {
			continue
		}

		go func(peerId int) {
			rf.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := rf.lastLogIndexAndTerm()
			rf.mu.Unlock()

			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  rf.me,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}
			reply := RequestVoteReply{}

			rf.dlog("sending RequestVote to %d: %+v", peerId, args)
			ok := rf.sendRequestVote(peerId, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.currentState != Candidate {
				rf.dlog("while waiting for reply, state=%v", rf.currentState)
				return
			}

			if reply.Term > savedCurrentTerm {
				rf.dlog("term out of date in RequestVoteReply")
				rf.becomeFollower(reply.Term)
			} else if reply.Term == savedCurrentTerm {
				if reply.VoteGranted {
					votes++
				}

				if votes*2 > len(rf.peers) {
					rf.dlog("got %d votes, becoming leader", votes)
					rf.becomeLeader()
					return
				}
			}

		}(peerId)
	}
}

func (rf *Raft) becomeLeader() {
	rf.currentState = Leader
	rf.lastHeartbeatEvent = time.Time{}
	// Volatile leader state to be reset.
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.tickFunc = rf.tickLeader
}

func (rf *Raft) tickLeader() {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	if time.Since(rf.lastHeartbeatEvent) <= time.Duration(80)*time.Millisecond {
		rf.mu.Unlock()
		return
	}
	savedCurrentTerm := rf.currentTerm
	rf.lastHeartbeatEvent = time.Now()
	rf.mu.Unlock()

	for peerId := range rf.peers {
		if peerId == rf.me {
			continue
		}

		go func(peerId int) {
			rf.mu.Lock()
			nextIndex := rf.nextIndex[peerId]
			prevLogIndex := nextIndex - 1
			prevLogTerm := rf.log[prevLogIndex].Term
			entries := rf.log[nextIndex:]

			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			rf.dlog("sending AppendEntries to %v: nextIndex=%d, args=%+v, log=%+v",
				peerId, nextIndex, args, rf.log)
			var reply AppendEntriesReply

			ok := rf.sendAppendEntries(peerId, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > savedCurrentTerm {
				rf.dlog("term out of date in heartbeat reply")
				rf.becomeFollower(reply.Term)
				return
			}

			if rf.currentState == Leader && savedCurrentTerm == reply.Term {
				if reply.Success {
					rf.nextIndex[peerId] = nextIndex + len(entries)
					rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1
					rf.dlog("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v",
						peerId, rf.nextIndex, rf.matchIndex)

					savedCommitIndex := rf.commitIndex
					// Try to find the highest commitIndex.
					for i := rf.commitIndex + 1; i < len(rf.log); i++ {
						if rf.log[i].Term == rf.currentTerm {
							matchCount := 1
							for _, mi := range rf.matchIndex {
								if mi >= i {
									matchCount++
								}
							}
							if matchCount*2 > len(rf.peers) {
								rf.commitIndex = i
							}
						}
					}

					if rf.commitIndex != savedCommitIndex {
						rf.dlog("leader sets commitIndex := %d", rf.commitIndex)
						// Channel used internally by the CM to signal that new entries are ready to
						// be on the commit channel to the client?
						rf.newCommitReadyChan <- struct{}{}
					}
				} else {
					// Update nextIndex for follower since it failed.
					if reply.ConflictTerm >= 1 {
						lastIndexOfTerm := 0
						for i := len(rf.log) - 1; i >= 0; i-- {
							if rf.log[i].Term == reply.ConflictTerm {
								lastIndexOfTerm = i
								break
							}
						}
						if lastIndexOfTerm >= 1 {
							rf.nextIndex[peerId] = lastIndexOfTerm + 1
						} else {
							rf.nextIndex[peerId] = reply.ConflictIndex
						}
					} else {
						rf.nextIndex[peerId] = reply.ConflictIndex
					}
				}
			}
		}(peerId)
	}
}

func (rf *Raft) electionTimeout() time.Duration {
	return time.Duration(250+(rand.Int63()%250)) * time.Millisecond
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.log)
	term := rf.currentTerm
	isLeader := false

	rf.dlog("Start receieved by %v: %v", rf.currentState, command)
	if !rf.killed() && rf.currentState == Leader {
		entry := LogEntry{
			Term:    term,
			Command: command,
		}
		rf.log = append(rf.log, entry)
		isLeader = true
		rf.persist()
	}

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
	for !rf.killed() {
		// // Your code here (2A)
		// // Check if a leader election should be started.

		rf.mu.Lock()
		tickFunc := rf.tickFunc
		rf.mu.Unlock()
		tickFunc()

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) commitChanSender(applyCh chan ApplyMsg) {
	for range rf.newCommitReadyChan {
		rf.mu.Lock()
		savedLastApplied := rf.lastApplied
		var entries []LogEntry
		if rf.commitIndex > rf.lastApplied {
			entries = rf.log[rf.lastApplied+1 : rf.commitIndex+1]
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()
		rf.dlog("commitChanSender entries=%v, savedLastApplied=%d, currentTerm=%d", entries, savedLastApplied, rf.currentTerm)

		for i, entry := range entries {
			applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: savedLastApplied + i + 1,
			}
		}
	}
	rf.dlog("commitChanSender done")
}

func (rf *Raft) dlog(format string, args ...interface{}) {
	debug := true
	debug = false
	if debug {
		format = fmt.Sprintf("[%d] ", rf.me) + format + "\n"
		fmt.Printf(format, args...)
	}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.log = []LogEntry{{Term: 0}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.newCommitReadyChan = make(chan struct{})
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.becomeFollower(rf.currentTerm)

	// start ticker goroutine to start elections
	go rf.commitChanSender(applyCh)
	go rf.ticker()

	return rf
}
