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

const (
	DEBUG_MODE       = 0
	HeartbeatTimeout = 100 * time.Millisecond
	AppendTimeout    = 20 * time.Millisecond
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

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

func (rs RaftState) String() string {
	return []string{
		"Follower",
		"Candidate",
		"Leader",
	}[rs]
}

type LogEntry struct {
	Term    int
	Index   int
	Command any
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
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	// Internals.
	snapshot           []byte
	currentState       RaftState
	newCommitReadyChan chan struct{}
	applyCh            chan ApplyMsg
	lastElectionEvent  time.Time
	lastHeartbeatEvent time.Time
	tickFunc           func()
}

func (rf *Raft) getFirstIndex() int {
	return rf.log[0].Index
}

// lastIncludedTerm
func (rf *Raft) getFirstTerm() int {
	return rf.log[0].Term
}

func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getNextIndex() int {
	return rf.getLastIndex() + 1
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftState := w.Bytes()
	rf.dlog("saving snapshot (len:%+v)", len(rf.snapshot))
	rf.persister.Save(raftState, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data, snapshot []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("something happened, could not decode persisted state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.commitIndex = log[0].Index
		rf.lastApplied = log[0].Index
		rf.snapshot = snapshot
		rf.dlog("restore success, commitIndex/lastApplied=%d")
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastIncludedIndex := rf.getFirstIndex()
	if lastIncludedIndex >= index {
		return
	}

	rf.snapshot = snapshot
	rf.log = rf.log[index-lastIncludedIndex:]
	rf.log[0].Command = nil
	rf.persist()
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() {
		return
	}

	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	rf.dlog("RequestVote: received=%+v, currentTerm=%d", *args, rf.currentTerm)
	if args.Term > rf.currentTerm {
		rf.dlog("... RequestVote: current term is outdated, becoming follower")
		rf.becomeFollower(args.Term)
	}

	lastLogIndex := rf.getLastIndex()
	lastLogTerm := rf.getLastTerm()

	// If votedFor is null or candidateId, and candidate's log is at least
	// as up-to-date as receiver's log, grant vote.
	if args.Term == rf.currentTerm &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		rf.lastElectionEvent = time.Now()
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.persist()
	}
	reply.Term = rf.currentTerm
	rf.dlog("... RequestVote: reply=%+v", *reply)
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
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}
	if args.PrevLogIndex < rf.getFirstIndex() {
		return
	}

	rf.lastElectionEvent = time.Now()

	rf.dlog("AppendEntries: received=%+v currentTerm=%d, log=%+v", *args, rf.currentTerm, rf.log)
	if args.Term > rf.currentTerm {
		rf.dlog("... AppendEntries: current term is outdated, becoming follower")
		rf.becomeFollower(args.Term)
	}

	if args.Term == rf.currentTerm &&
		(args.PrevLogIndex < rf.getNextIndex() && rf.log[args.PrevLogIndex-rf.getFirstIndex()].Term == args.PrevLogTerm) {
		if rf.currentState != Follower {
			rf.becomeFollower(args.Term)
		}

		insertIndex := args.PrevLogIndex + 1
		entryIndex := 0

		for {
			if insertIndex >= rf.getNextIndex() || entryIndex >= len(args.Entries) {
				break
			}
			if rf.log[insertIndex-rf.getFirstIndex()].Term != args.Entries[entryIndex].Term {
				break
			}
			insertIndex++
			entryIndex++
		}

		rf.dlog("commitIndex=%d, log=%+v", rf.commitIndex, rf.log)

		// We're either
		// - at the end of the local log
		// - at the end of the entries
		// - or we found a mismatch somewhere
		rf.log = append(rf.log[:insertIndex-rf.getFirstIndex()], args.Entries[entryIndex:]...)
		rf.persist()
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
			// We send this over a goroutine so we don't block, found this
			// out the hard way.
			go func() {
				rf.newCommitReadyChan <- struct{}{}
			}()
		}
		reply.Success = true
	} else if args.PrevLogIndex >= rf.getNextIndex() {
		reply.ConflictIndex = rf.getNextIndex()
		reply.ConflictTerm = 0
	} else if args.PrevLogIndex < rf.getNextIndex() {
		reply.ConflictTerm = rf.log[args.PrevLogIndex-rf.getFirstIndex()].Term
		for i, msg := range rf.log {
			if msg.Term == reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}
	}

	reply.Term = rf.currentTerm
	rf.dlog("... AppendEntries: reply=%+v", *reply)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.LastIncludedIndex < rf.getFirstIndex() || args.LastIncludedIndex < rf.commitIndex {
		return
	}

	rf.lastElectionEvent = time.Now()

	rf.dlog("InstallSnapshot: received=%+v, currentTerm=%d", *args, rf.currentTerm)
	if args.Term > rf.currentTerm {
		rf.dlog("... InstallSnapshot: current term is outdated, becoming follower")
		rf.becomeFollower(args.Term)
	}

	if args.LastIncludedIndex < rf.getNextIndex() &&
		rf.log[args.LastIncludedIndex-rf.getFirstIndex()].Term == args.LastIncludedTerm {
		rf.log = rf.log[args.LastIncludedIndex-rf.getFirstIndex():]
	} else {
		rf.log = make([]LogEntry, 1)
	}
	rf.log[0].Term = args.LastIncludedTerm
	rf.log[0].Index = args.LastIncludedIndex

	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.snapshot = args.Data

	rf.persist()

	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2B).
	index := rf.getNextIndex()
	term := rf.currentTerm
	isLeader := rf.currentState == Leader

	if isLeader {
		rf.dlog("Start: command=%v [term=%d]", command, term)
		rf.log = append(rf.log, LogEntry{
			Term:    term,
			Index:   index,
			Command: command,
		})
		rf.persist()
		// We set a shorter timeout here, so we can increase throughput.
		// I haven't found a better way to address this, so this will have
		// to do.
		rf.lastHeartbeatEvent = time.Now().Add(-HeartbeatTimeout + AppendTimeout)
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

// State machine stuff.

func (rf *Raft) becomeFollower(term int) {
	rf.currentState = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.lastElectionEvent = time.Now()
	rf.tickFunc = rf.tickFollower
	rf.persist()
}

func (rf *Raft) tickFollower() {
	if !rf.killed() && time.Since(rf.lastElectionEvent) > rf.electionTimeout() {
		rf.dlog("follower exceeded election timeout, becoming candidate")
		rf.becomeCandidate()
	}
}

func (rf *Raft) becomeCandidate() {
	rf.currentState = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastElectionEvent = time.Now().Add(-1 * time.Second)
	rf.tickFunc = rf.tickCandidate
	rf.persist()
}

func (rf *Raft) tickCandidate() {
	if !rf.killed() && time.Since(rf.lastElectionEvent) > rf.electionTimeout() {
		rf.currentTerm++
		rf.candidateRequestVotes()
	}
}

func (rf *Raft) candidateRequestVotes() {
	savedCurrentTerm := rf.currentTerm
	rf.lastElectionEvent = time.Now()
	votesReceived := 1

	rf.dlog("candidate requesting votes, currentTerm=%d", savedCurrentTerm)
	for peerId := range rf.peers {
		if peerId == rf.me {
			continue
		}
		go func(peerId int) {
			rf.mu.Lock()
			lastLogIndex := rf.getLastIndex()
			lastLogTerm := rf.getLastTerm()
			rf.mu.Unlock()

			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			var reply RequestVoteReply

			if !rf.sendRequestVote(peerId, &args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.currentState != Candidate {
				rf.dlog("... while waiting for RequestVote reply, changed state=%v", rf.currentState)
				return
			}
			if reply.Term > savedCurrentTerm {
				rf.dlog("... while waiting for RequestVote reply, term out of date")
				return
			}

			if reply.Term == rf.currentTerm {
				if reply.VoteGranted {
					votesReceived++
					if votesReceived*2 > len(rf.peers) {
						rf.dlog("... wins election with %d votes, becoming leader", votesReceived)
						rf.becomeLeader()
						return
					}
				}
			}
		}(peerId)
	}
}

func (rf *Raft) becomeLeader() {
	rf.currentState = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.getNextIndex()
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.lastHeartbeatEvent = time.Now().Add(-HeartbeatTimeout)
	rf.tickFunc = rf.tickLeader
}

func (rf *Raft) tickLeader() {
	if !rf.killed() && time.Since(rf.lastHeartbeatEvent) > HeartbeatTimeout {
		rf.leaderAppendEntries()
	}
}

func (rf *Raft) leaderAppendEntries() {
	rf.lastHeartbeatEvent = time.Now()
	savedCurrentTerm := rf.currentTerm

	rf.dlog("leader appending entries, currentTerm=%d", savedCurrentTerm)
	for peerId := range rf.peers {
		if peerId == rf.me {
			continue
		}
		go func(peerId int) {
			rf.mu.Lock()
			lastIncludedIndex := rf.getFirstIndex()
			nextIndex := rf.nextIndex[peerId]

			if nextIndex <= lastIncludedIndex {
				args := InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.getFirstIndex(),
					LastIncludedTerm:  rf.getFirstTerm(),
					Data:              rf.snapshot,
				}
				var reply InstallSnapshotReply
				rf.mu.Unlock()

				if ok := rf.sendInstallSnapshot(peerId, &args, &reply); ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > savedCurrentTerm {
						rf.dlog("... skipping AppendEntries reply since term out of date")
						rf.becomeFollower(reply.Term)
					}
					rf.matchIndex[peerId] = max(rf.matchIndex[peerId], args.LastIncludedIndex)
					rf.nextIndex[peerId] = rf.matchIndex[peerId] + 1
				}
				return
			}

			prevLogIndex := nextIndex - 1
			prevLogTerm := rf.log[prevLogIndex-rf.getFirstIndex()].Term
			copyLog := make([]LogEntry, rf.getLastIndex()+1-nextIndex)
			copy(copyLog, rf.log[nextIndex-rf.getFirstIndex():])

			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      copyLog,
				LeaderCommit: rf.commitIndex,
			}
			var reply AppendEntriesReply
			rf.mu.Unlock()

			if !rf.sendAppendEntries(peerId, &args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.currentState != Leader {
				rf.dlog("... while waiting for AppendEntries reply, changed state=%v", rf.currentState)
				return
			}

			if reply.Term > savedCurrentTerm {
				rf.dlog("... skipping AppendEntries reply since term out of date")
				rf.becomeFollower(reply.Term)
			}

			if reply.Term == savedCurrentTerm {
				if reply.Success {
					rf.nextIndex[peerId] = nextIndex + len(args.Entries)
					rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1
					savedCommitIndex := rf.commitIndex

					for i := rf.commitIndex + 1; i < rf.getNextIndex(); i++ {
						if rf.log[i-rf.getFirstIndex()].Term == savedCurrentTerm {
							counts := 1
							for _, mi := range rf.matchIndex {
								if mi >= i {
									counts++
								}
							}
							if counts*2 >= len(rf.peers) {
								rf.commitIndex = i
							}
						}
					}

					if rf.commitIndex > savedCommitIndex {
						// We send this over a goroutine so we don't block, found this
						// out the hard way.
						go func() {
							rf.newCommitReadyChan <- struct{}{}
						}()
					}
				} else {
					if reply.ConflictTerm != 0 {
						for i := rf.getLastIndex(); i > rf.getFirstIndex(); i-- {
							if rf.log[i-rf.getFirstIndex()].Term == reply.ConflictTerm {
								rf.dlog("setting nextIndex[%d]=%d", peerId, i+1)
								rf.nextIndex[peerId] = i + 1
								return
							}
						}
						rf.dlog("setting nextIndex[%d]=%d", peerId, reply.ConflictIndex)
						rf.nextIndex[peerId] = min(rf.matchIndex[peerId]+1, reply.ConflictIndex)
					} else {
						rf.dlog("setting nextIndex[%d]=%d", peerId, reply.ConflictIndex)
						rf.nextIndex[peerId] = min(rf.matchIndex[peerId]+1, reply.ConflictIndex)
					}
				}
			}
		}(peerId)
	}
}

func (rf *Raft) electionTimeout() time.Duration {
	ms := 300 + (rand.Int63() % 300)
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		rf.tickFunc()
		rf.mu.Unlock()

		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

func (rf *Raft) newCommitChanSender(applyChan chan ApplyMsg) {
	for range rf.newCommitReadyChan {
		rf.mu.Lock()
		entries := rf.log[rf.lastApplied+1-rf.getFirstIndex() : rf.commitIndex+1-rf.getFirstIndex()]
		lastApplied := rf.lastApplied
		commitIndex := rf.commitIndex
		rf.dlog("committed entries=%+v, new log=%+v", entries, rf.log)
		rf.mu.Unlock()

		for i, log := range entries {
			applyChan <- ApplyMsg{
				CommandValid: true,
				Command:      log.Command,
				CommandIndex: lastApplied + i + 1,
			}
		}

		rf.mu.Lock()
		if commitIndex > rf.lastApplied {
			rf.lastApplied = commitIndex
		}
		rf.mu.Unlock()
	}
	close(rf.newCommitReadyChan)
}

func (rf *Raft) dlog(format string, args ...any) {
	if DEBUG_MODE > 0 {
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
	rf.log = []LogEntry{{}}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.newCommitReadyChan = make(chan struct{})
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.getNextIndex()
	}
	rf.becomeFollower(rf.currentTerm)

	// start ticker goroutine to start elections
	go rf.newCommitChanSender(applyCh)
	go rf.ticker()

	return rf
}
