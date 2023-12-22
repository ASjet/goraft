package raft

import "fmt"

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	Candidate    int
	LastLogIndex int
	LastLogTerm  int
}

func (s *RequestVoteArgs) String() string {
	return fmt.Sprintf("T:%d,C:%d,LI:%d,LT:%d",
		s.Term, s.Candidate, s.LastLogIndex, s.LastLogTerm)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term    int
	Granted bool
}

func (s *RequestVoteReply) String() string {
	return fmt.Sprintf("T:%d,G:%v", s.Term, s.Granted)
}

type AppendEntriesArgs struct {
	Term         int
	Leader       int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

func (s *AppendEntriesArgs) String() string {
	if len(s.Entries) > 0 {
		return fmt.Sprintf("T:%d,E:%d,PI:%d,PT:%d,C:%d",
			s.Term, len(s.Entries), s.PrevLogIndex, s.PrevLogTerm, s.LeaderCommit)
	}
	return fmt.Sprintf("T:%d,C:%d,Heartbeat", s.Term, s.LeaderCommit)
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	LastLogIndex int
	LastLogTerm  int
}

func (s *AppendEntriesReply) String() string {
	return fmt.Sprintf("T:%d,S:%v,LI:%d,LT:%d",
		s.Term, s.Success, s.LastLogIndex, s.LastLogTerm)
}
