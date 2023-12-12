package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term      int
	Candidate int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term    int
	Granted bool
}

type AppendEntriesArgs struct {
	Term   int
	Leader int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}
