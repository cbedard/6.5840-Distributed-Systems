package raft

// RequestVoteArgs example RequestVote RPC arguments structure. field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	//false if term < currentTerm
	Term int
	// true means the candidate received the vote
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogEntry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int // for fast backup
}

// skipping offset & done fields on the advice of the hint section in 3D
type InstallSnapshotArgs struct {
	Term         int
	LeaderId     int
	LastLogIndex int
	LastLogTerm  int
	Data         []byte
}

type InstallSnapshotReply struct {
	Term int
}
