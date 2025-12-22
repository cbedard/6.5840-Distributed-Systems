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
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// ApplyMsg as each Raft peer becomes aware that successive log entries are  committed, the peer should send an
// ApplyMsg to the service (or tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g., snapshots) on the applyCh, but set
// CommandValid to false for these other uses.
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

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C). Look at the paper's Figure 2.
	//Persistent state
	currentTerm int
	votedFor    int
	log         []ApplyMsg

	//Volatile state, init to 0
	commitIndex int
	lastApplied int

	//Volatile leader state
	nextIndex  []int //init leader lastApplied + 1
	matchIndex []int //init 0

	lastHeartbeatTime time.Time
	state             string //LEADER, FOLLOWER, CANDIDATE
}

// GetState returns the currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == "LEADER"
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

// Snapshot the service says it has created a snapshot that has all info up to and including index.
// this means the service no longer needs the log through (and including) that index.
// Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "FOLLOWER"
		rf.votedFor = -1 // Reset vote for the new term
	}

	// Then decide whether to grant vote
	if args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		// TODO: Also check log is up-to-date (for later)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastHeartbeatTime = time.Now() // Reset election timer
	}

	reply.Term = rf.currentTerm
}

// Example code to send a RequestVote RPC to a server, server is the index of the target server in rf.peers[].
// Expects RPC arguments in args. Fills in *reply with RPC reply, so the caller should pass &reply.
// The types of the args and reply passed to Call() must be the same as the types of the arguments
// declared in the handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers may be unreachable, and in which
// requests and replies may be lost. Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise Call() returns false. Thus Call() may
// not return for a while. A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the handler function on the server
// side does not return. Thus, there is no need to implement your own timeouts around Call().
// look at the comments in ../labrpc/labrpc.go for more details.
//
// If you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	retries := 0
	for !ok && retries < 3 {
		time.Sleep(20 * time.Millisecond)
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
		retries++
	}

	//fmt.Println(timeSinceStart(), "term", rf.currentTerm, "sendRequestVote from", rf.me, "to", server, ":", reply.VoteGranted, "in", retries, "retries")
	if reply.VoteGranted {
		return true
	}
	return false
}

func (rf *Raft) broadcastHeartbeat() {
	for i := range rf.peers {
		if i != rf.me {
			go func(peer, term, leaderId, prevLogIndex, leaderCommitIndex int, entries []interface{}) {
				_ = rf.SendAppendEntries(peer,
					//TODO: some of these fields aren't accurate yet, will update in 3b
					&AppendEntriesArgs{term, leaderId, prevLogIndex, term, entries, leaderCommitIndex},
					&AppendEntriesReply{},
				)
			}(i, rf.currentTerm, rf.me, 0, 0, nil)
		}
	}
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	retries := 0
	for !ok && retries < 3 {
		time.Sleep(20 * time.Millisecond)
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		retries++
	}
	return ok
}

// gatherVotes sends vote request to all neighbors and returns whether we have won an election
func (rf *Raft) gatherVotes() bool {
	votesChan := make(chan bool, len(rf.peers))
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	// Send RequestVote RPCs to all peers
	rf.mu.Lock()
	for i := range rf.peers {
		if rf.me != i {
			go func(peer, term, candidateId, lastLogIndex, lastLogTerm int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(peer,
					//TODO:replace last arg with rf.log[len(rf.log)].SnapshotTerm
					&RequestVoteArgs{term, candidateId, lastLogIndex, lastLogTerm},
					&reply,
				)

				select {
				case votesChan <- ok && reply.VoteGranted:
				case <-ctx.Done():
				}
			}(i, rf.currentTerm, rf.me, rf.lastApplied, 0)
		}
	}
	rf.mu.Unlock()

	// Wait for votes with early termination
	yesVotes := 1
	votesReceived := 1
	requiredVotes := len(rf.peers)/2 + 1

	for votesReceived < len(rf.peers) {
		select {
		case vote := <-votesChan:
			votesReceived++
			if vote {
				yesVotes++
				if yesVotes >= requiredVotes {
					cancel()
					return true
				}
			}

			// Check if we can still win
			remaining := len(rf.peers) - votesReceived
			if yesVotes+remaining < requiredVotes {
				cancel()
				return false
			}

		case <-ctx.Done():
			return yesVotes >= requiredVotes
		}
	}

	return yesVotes >= requiredVotes
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	/*
		Receiver implementation:
		yes 1. Reply false if term < currentTerm (§5.1)
		yes 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		yes 3. If an existing entry conflicts with a new one (same index but different terms), delete the
			existing entry and all that follow it (§5.3)
		yes 4. Append any new entries not already in log
		5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	*/
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm || (args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex].SnapshotTerm != args.PrevLogTerm) {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex].SnapshotTerm != args.PrevLogTerm {
		// delete mismatched log and everything after it
		rf.log = rf.log[:args.PrevLogIndex-1]
	}

	fmt.Println("node", rf.me, rf.state, "log length:", len(rf.log), rf.commitIndex, "args prevLogIndex:", args.PrevLogIndex)
	for _, entry := range args.Entries {
		newMsg := ApplyMsg{
			true,
			entry,
			rf.commitIndex + 1,
			true,
			[]byte{},
			args.Term,
			rf.commitIndex + 1,
		}
		rf.commitIndex++
		rf.log = append(rf.log, newMsg)
	}

	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommitIndex, rf.commitIndex)
	}
	//TODO: if term is higher that means we likely missed entries
	rf.lastHeartbeatTime = time.Now()
	rf.state = "FOLLOWER"
	rf.currentTerm = args.Term
	rf.votedFor = -1

	reply.Success = true
	reply.Term = rf.currentTerm
}

// Start the service using Raft (e.g., a k/v server) wants to start agreement on the next command
// to be appended to Raft's log. If this server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this command will ever be committed
// to the Raft log, since the leader may fail or lose an election. Even if the Raft instance has been
// killed, this function should return gracefully. The first return value is the index that the command
// will appear at if it's ever committed. The second return value is the current term. The third return
// value is true if this server believes it is the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
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
		rf.mu.Lock()
		if rf.state == "FOLLOWER" {
			prevTime := rf.lastHeartbeatTime
			rf.mu.Unlock()

			// Pause for a random amount of time between 250 and 400 milliseconds.
			ms := 250 + (rand.Int63() % 200)
			time.Sleep(time.Duration(ms) * time.Millisecond)

			rf.mu.Lock()
			if prevTime == rf.lastHeartbeatTime {
				//start election
				//fmt.Println(timeSinceStart(), "election started by:", rf.me)
				rf.state = "CANDIDATE"
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.mu.Unlock()

				electionWon := rf.gatherVotes()

				rf.mu.Lock()
				if electionWon && rf.state == "CANDIDATE" {
					rf.state = "LEADER"
					//fmt.Println(timeSinceStart(), rf.currentTerm, "election WON by:", rf.me)
					rf.broadcastHeartbeat()
				} else {
					rf.state = "FOLLOWER"
					//fmt.Println(timeSinceStart(), rf.currentTerm, "election FAIL by:", rf.me)
				}
				rf.votedFor = -1
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
			}
		} else {
			rf.broadcastHeartbeat()
			rf.mu.Unlock()
			time.Sleep(110 * time.Millisecond)
		}
	}
}

// Make the service or tester wants to create a Raft server. The ports of all the Raft servers
// (including this one) are in peers[]. This server's port is peers[me]. All the servers' peers[]
// arrays have the same order. Persister is a place for this server to Save its persistent state and
// also initially holds the most recent saved state, if any. applyCh is a channel on which the tester
// or service expects Raft to send ApplyMsg messages. Make() must return quickly, so it should start
// goroutines for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.lastHeartbeatTime = time.Now()
	rf.state = "FOLLOWER"
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
