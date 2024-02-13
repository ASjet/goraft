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
	"bytes"
	"sync"
	"sync/atomic"

	"goraft/src/labgob"
	"goraft/src/labrpc"
	"goraft/src/models"
	"goraft/src/util/log"
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
	SnapshotTerm  models.Term
	SnapshotIndex int
}

type persistState struct {
	Term models.Term
	Vote int
	Logs []models.Log
}

// A Go object implementing a single Raft peer.
type Raft struct {
	// Immutable states
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// External mutable states
	dead    atomic.Bool // set by Kill()
	applyCh chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Internal mutable states
	stateMu sync.Locker
	state   State

	logCond       *sync.Cond   // Must hold this lock when accessing following fields
	snapshotIndex int          // The index of the first log entry in logs
	logs          []models.Log // The actually log entries, the first elem is a dummy entry
	commitIndex   int          // The index of highest log entry known to be committed
	snapshot      []byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.state.Lock()
	defer rf.state.Unlock()
	return int(rf.state.Term()), rf.state.Role() == RoleLeader
}

func (rf *Raft) dumpState() []byte {
	ps := &persistState{
		Term: rf.state.Term(),
		Vote: rf.state.Voted(),
		Logs: rf.logs,
	}
	buf := new(bytes.Buffer)
	if err := labgob.NewEncoder(buf).Encode(ps); err != nil {
		log.Fatal("Persist state failed: %s", err)
	}
	return buf.Bytes()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persistState() {
	// Your code here (2C).
	rf.persister.SaveRaftState(rf.dumpState())
}

func (rf *Raft) persistSnapshot(snapshot []byte) {
	// Your code here (2D).
	rf.persister.SaveStateAndSnapshot(rf.dumpState(), snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.state = Follower(0, NoVote, Base(rf))
		return
	}
	// Your code here (2C).
	ps := new(persistState)
	if err := labgob.NewDecoder(bytes.NewBuffer(data)).Decode(ps); err != nil {
		rf.state = Follower(0, NoVote, Base(rf))
		return
	}

	rf.state = Follower(ps.Term, ps.Vote, Base(rf))
	rf.logs = ps.Logs
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm models.Term, lastIncludedIndex int, snapshot []byte) bool {
	// Always return true here
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	log.Debug("%s make on demand snapshot at index %d", rf.state, index)
	defer log.Debug("%s made on demand snapshot at index %d successfully", rf.state, index)
	// Your code here (2D).
	// NOTE: no need to hold rf.state.Lock() and will cause deadlock
	rf.state.LockLog()
	defer rf.state.UnlockLog()

	actualIndex := index - rf.snapshotIndex
	if actualIndex <= 0 {
		// The snapshot is too old
		log.Info("%s drop old snapshot at index %d", rf.state, index)
		return
	}

	lastIndex := rf.state.LastLogIndex()

	if index > lastIndex {
		log.Info("%s on demand snapshot covered all logs, drop all", rf.state)
		rf.logs = []models.Log{{rf.state.Term(), nil}}
	} else {
		log.Info("%s on demand snapshot covered part logs, drop [:%d]", rf.state, actualIndex)
		rf.logs = rf.logs[actualIndex:]
	}

	if rf.commitIndex < index {
		rf.commitIndex = index
	}
	rf.snapshot = snapshot
	rf.snapshotIndex = index
	log.Info("%s make on demand snapshot at index %d", rf.state, index)
	rf.persistSnapshot(snapshot)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *models.RequestVoteArgs, reply *models.RequestVoteReply) {
	// Your code here (2A, 2B).
	log.Debug("%s RPC RequestVote from %d", rf.state, args.Candidate)
	rf.state.Lock()
	defer rf.state.Unlock()
	defer log.Debug("%s RPC RequestVote returned to %d", rf.state, args.Candidate)

	reply.Term = rf.state.Term()

	if args.Term < reply.Term {
		// Reply false if term < currentTerm (§5.1)
		reply.Granted = false
		return
	}

	if args.Term > reply.Term {
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		// We don't valid the log entries here since we won't vote for it
		if rf.state.Close("receive higher term %d from %d, revert to follower",
			args.Term, args.Candidate) {
			rf.state.To(Follower(args.Term, NoVote, rf.state))
		}
	}

	reply.Granted = rf.state.RequestVote(args)
}

func (rf *Raft) AppendEntries(args *models.AppendEntriesArgs, reply *models.AppendEntriesReply) {
	log.Debug("%s RPC AppendEntries from %d", rf.state, args.Leader)
	rf.state.Lock()
	defer rf.state.Unlock()
	defer log.Debug("%s RPC AppendEntries returned to %d", rf.state, args.Leader)

	reply.Term = rf.state.Term()

	if args.Term < reply.Term {
		// Reply false if term < currentTerm (§5.1)
		reply.Success = false
		return
	}

	if args.Term > reply.Term {
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		// We don't valid the log entries here since we won't vote for it
		if rf.state.Close("receive higher term %d from %d, revert to follower",
			args.Term, args.Leader) {
			rf.state.To(Follower(args.Term, NoVote, rf.state))
		}
	}

	reply.Success = rf.state.AppendEntries(args)
	rf.state.LockLog()
	index, log := rf.state.GetLog(-1)
	if log != nil {
		reply.LastLogIndex, reply.LastLogTerm = index, log.Term
	}
	rf.state.UnlockLog()
}

func (rf *Raft) InstallSnapshot(args *models.InstallSnapshotArgs, reply *models.InstallSnapshotReply) {
	log.Debug("%s RPC InstallSnapshot from %d", rf.state, args.Leader)
	rf.state.Lock()
	defer rf.state.Unlock()
	defer log.Debug("%s RPC InstallSnapshot returned to %d", rf.state, args.Leader)

	reply.Term = rf.state.Term()

	// 1. Reply immediately if term < currentTerm
	if args.Term < reply.Term {
		return
	}

	if args.Term > reply.Term {
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		// We don't valid the log entries here since we won't vote for it
		if rf.state.Close("receive higher term %d from %d, revert to follower",
			args.Term, args.Leader) {
			rf.state.To(Follower(args.Term, NoVote, rf.state))
		}
	}

	rf.state.InstallSnapshot(args)
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
	term := models.Term(-1)
	isLeader := false

	// Your code here (2B).
	rf.state.Lock()
	defer rf.state.Unlock()

	if isLeader = rf.state.Role() == RoleLeader; isLeader {
		index, term = rf.state.AppendCommand(command)
	}

	return index, int(term), isLeader
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
	rf.dead.Store(true)
	// Your code here, if desired.
	rf.state.Lock()
	rf.state.Close("killed")
	rf.state.Unlock()
}

func (rf *Raft) killed() bool {
	return rf.dead.Load()
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
	rf.logs = []models.Log{{0, nil}}
	rf.applyCh = applyCh
	rf.stateMu = log.LockerWithTrace(int64(me), new(sync.Mutex))
	rf.logCond = sync.NewCond(log.LockerWithTrace(int64(me), new(sync.Mutex)))

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
