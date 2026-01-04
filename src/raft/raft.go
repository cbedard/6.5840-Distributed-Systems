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
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// ApplyMsg as each Raft peer becomes aware that successive log entries are  committed, the peer should send an
// ApplyMsg to the service (or tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly committed log entry.
// in part 3D you'll want to send other kinds of messages (e.g., snapshots) on the applyCh, but set
// CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command any
}

// Snapshot holds snapshot metadata and data
type Snapshot struct {
	Index int
	Term  int
	Data  []byte
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC endpoints of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry // log entries after snapshot
	snap        Snapshot   // current snapshot (Index, Term, Data)

	// Volatile state
	commitIndex int       // index of most recent commit (on this peer)
	lastApplied int       // index of lastApplied commit to commitCh, <= commitIndex
	pendingSnap *Snapshot // snapshot waiting to be sent to service via applyCh

	// Volatile leader-specific state
	nextIndex  []int // for each server, index of next log entry to send
	matchIndex []int // for each server, index of highest log entry known to be replicated

	lastHeartbeatTime time.Time
	state             string        // LEADER, FOLLOWER, CANDIDATE
	commitCh          chan ApplyMsg // used by tester to confirm a commited log
	applyCond         *sync.Cond    // signals applier goroutine
}

// GetState returns the currentTerm and whether this server believes it is the leader. Used by tester.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == "LEADER"
}

// persist Raft's persistent state to stable storage, where it can later be retrieved after a crash and restart.
// is meant to capture value changes in Raft struct so should be called while holding rf.mu
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.snap.Index)
	e.Encode(rf.snap.Term)
	e.Encode(rf.log)

	rf.persister.Save(w.Bytes(), rf.snap.Data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm, votedFor, snapIndex, snapTerm int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&snapIndex) != nil || d.Decode(&snapTerm) != nil || d.Decode(&log) != nil {
		panic("ERROR Decoding")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.snap.Index = snapIndex
	rf.snap.Term = snapTerm
	rf.log = log
}

// Snapshot the service says it has created a snapshot that has all info up to and including index.
// The service no longer needs the log through (and including) that index, trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Get term before trimming
	snapshotTerm := rf.log[rf.toPhysical(index)].Term
	rf.log = rf.log[rf.toPhysical(index)+1:]
	rf.snap = Snapshot{Index: index, Term: snapshotTerm, Data: snapshot}
	rf.persist()
}

// InstallSnapshot RPC handler - follower receives snapshot from leader
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || args.LastLogIndex <= rf.snap.Index {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "FOLLOWER"
		rf.votedFor = -1
	}
	rf.lastHeartbeatTime = time.Now()

	// Set pending snapshot for applier to send to service
	newSnap := snapshotFromArgs(args)
	rf.pendingSnap = &newSnap

	// Trim log: keep entries after snapshot, or clear if snapshot covers everything
	physicalIndex := rf.toPhysical(args.LastLogIndex) + 1
	if physicalIndex < len(rf.log) {
		rf.log = rf.log[physicalIndex:]
	} else {
		rf.log = []LogEntry{}
	}

	rf.snap = newSnap
	rf.commitIndex = max(rf.commitIndex, args.LastLogIndex)
	rf.lastApplied = args.LastLogIndex

	rf.persist()
	rf.applyCond.Signal()
}

// sendInstallSnapshot sends snapshot to a peer - called inside broadcastEntries() which holds lock
func (rf *Raft) sendInstallSnapshot(peer int) {
	args := &InstallSnapshotArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LastLogIndex: rf.snap.Index,
		LastLogTerm:  rf.snap.Term,
		Data:         rf.snap.Data,
	}

	go func(peer int, args *InstallSnapshotArgs) {
		reply := &InstallSnapshotReply{}
		ok := rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)

		if ok {
			rf.mu.Lock()
			// Step down if we're behind
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = "FOLLOWER"
				rf.votedFor = -1
				rf.persist()
			} else if rf.state == "LEADER" && rf.currentTerm == args.Term {
				rf.nextIndex[peer] = args.LastLogIndex + 1
				rf.matchIndex[peer] = args.LastLogIndex
			}
			rf.mu.Unlock()
		}
	}(peer, args)
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "FOLLOWER"
		rf.votedFor = -1
		rf.persist()
	}

	// check if candidate log is up to date (§5.4.1)
	logOk := args.LastLogTerm > rf.lastLogTerm() ||
		(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex())

	if args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && logOk {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastHeartbeatTime = time.Now()
		rf.persist()
	}

	reply.Term = rf.currentTerm
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok && reply.VoteGranted
}

// broadcastEntries is always called with rf.mu held
func (rf *Raft) broadcastEntries() {
	for i := range rf.peers {
		if i != rf.me {
			nextIdx := rf.nextIndex[i]

			// If nextIdx is within curr snapshot, we need to InstallSnapshot
			if nextIdx <= rf.snap.Index {
				rf.sendInstallSnapshot(i)
				continue
			}

			prevLogIndex := nextIdx - 1
			var prevLogTerm int
			if prevLogIndex == rf.snap.Index {
				prevLogTerm = rf.snap.Term
			} else {
				prevLogTerm = rf.log[rf.toPhysical(prevLogIndex)].Term
			}

			entries := []LogEntry{}
			if nextIdx <= rf.lastLogIndex() {
				entries = append(entries, rf.log[rf.toPhysical(nextIdx):]...)
			}

			go func(peer, term, leaderId, prevLogIndex, prevLogTerm, leaderCommitIndex int, entries []LogEntry) {
				reply := &AppendEntriesReply{}
				success := rf.SendAppendEntries(peer,
					&AppendEntriesArgs{term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommitIndex},
					reply,
				)

				if success {
					rf.mu.Lock()
					// staleness check
					if rf.state == "LEADER" && rf.currentTerm == term {
						newMatchIndex := prevLogIndex + len(entries)
						rf.matchIndex[peer] = newMatchIndex
						rf.nextIndex[peer] = newMatchIndex + 1

						rf.advanceCommitIndex()
					}
					rf.mu.Unlock()
				}
			}(i, rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, rf.commitIndex, entries)
		}
	}
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if ok && !reply.Success {
		rf.mu.Lock()
		// Step down if we're behind
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = "FOLLOWER"
			rf.votedFor = -1
			rf.persist()
			rf.mu.Unlock()
			return false
		}
		// Back up nextIndex
		if reply.ConflictIndex > 0 {
			rf.nextIndex[server] = reply.ConflictIndex
		}
		rf.mu.Unlock()
	}

	return ok && reply.Success
}

// gatherVotes sends vote request to all neighbors and returns whether we have won an election
func (rf *Raft) gatherVotes() bool {
	votesChan := make(chan bool, len(rf.peers))
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	rf.mu.Lock()
	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.lastLogTerm()

	for i := range rf.peers {
		if rf.me != i {
			go func(peer, term, candidateId, lastLogIndex, lastLogTerm int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(peer,
					&RequestVoteArgs{term, candidateId, lastLogIndex, lastLogTerm},
					&reply,
				)

				select {
				case votesChan <- ok:
				case <-ctx.Done():
				}
			}(i, rf.currentTerm, rf.me, lastLogIndex, lastLogTerm)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reject stale term
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// Update term if newer
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "FOLLOWER"
		rf.votedFor = -1
		rf.persist()
	}

	// If PrevLogIndex is before our snapshot, tell leader to send from after our snapshot
	if args.PrevLogIndex < rf.snap.Index {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = rf.snap.Index + 1
		return
	}

	// Get the term at PrevLogIndex (could be in snapshot or log)
	var prevLogTerm int
	if args.PrevLogIndex == rf.snap.Index {
		prevLogTerm = rf.snap.Term
	} else if args.PrevLogIndex <= rf.lastLogIndex() {
		prevLogTerm = rf.log[rf.toPhysical(args.PrevLogIndex)].Term
	}

	// Log consistency check
	if args.PrevLogIndex > rf.lastLogIndex() || prevLogTerm != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = rf.commitIndex + 1
		return
	}

	// Append entries only if they do not already exist
	for i, entry := range args.Entries {
		physicalIndex := rf.toPhysical(args.PrevLogIndex + 1 + i)
		if physicalIndex < len(rf.log) {
			// Entry exists - check for conflict and truncate
			if rf.log[physicalIndex].Term != entry.Term {
				rf.log = rf.log[:physicalIndex]
				rf.log = append(rf.log, entry)
			}
		} else {
			rf.log = append(rf.log, entry)
		}
	}
	rf.persist()

	// Update commitIndex if leader is ahead
	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommitIndex, rf.lastLogIndex())
		rf.applyCond.Signal()
	}

	rf.lastHeartbeatTime = time.Now()
	rf.state = "FOLLOWER"

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
func (rf *Raft) Start(command any) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != "LEADER" {
		return 0, 0, false
	}

	newEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}

	rf.log = append(rf.log, newEntry)
	index := rf.lastLogIndex()
	term := rf.currentTerm
	rf.persist()

	// immediate broadcast -> faster replication
	rf.broadcastEntries()

	return index, term, true
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
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// applier is a dedicated goroutine that applies committed entries to the state machine.
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		if rf.pendingSnap != nil {
			// snapshots have priority
			msg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.pendingSnap.Data,
				SnapshotTerm:  rf.pendingSnap.Term,
				SnapshotIndex: rf.pendingSnap.Index,
			}
			rf.pendingSnap = nil

			rf.mu.Unlock()
			rf.commitCh <- msg
			rf.mu.Lock()

		} else if rf.commitIndex > rf.lastApplied {
			// normal commits
			entries := append([]LogEntry{}, rf.log[rf.toPhysical(rf.lastApplied+1):rf.toPhysical(rf.commitIndex+1)]...)
			startIdx := rf.lastApplied
			rf.lastApplied = rf.commitIndex

			rf.mu.Unlock()
			for i, entry := range entries {
				rf.commitCh <- ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: startIdx + 1 + i,
				}
			}
			rf.mu.Lock()

		} else {
			rf.applyCond.Wait()
			if rf.killed() {
				return
			}
		}
	}
}

// advanceCommitIndex checks if commitIndex can be advanced, leader-only
// caller must hold rf.mu. signals applier if commitIndex changes.
func (rf *Raft) advanceCommitIndex() {
	// Iterate forward from commitIndex+1 to find highest committed
	for n := rf.commitIndex + 1; n <= rf.lastLogIndex(); n++ {
		if rf.log[rf.toPhysical(n)].Term != rf.currentTerm {
			continue
		}
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
		} else {
			break // If n isn't committed, n+1 won't be either
		}
	}
	rf.applyCond.Signal()
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == "FOLLOWER" {
			prevTime := rf.lastHeartbeatTime
			rf.mu.Unlock()

			// Pause for a random amount of time between 250 and 400 milliseconds.
			ms := 250 + (rand.Int63() % 150)
			time.Sleep(time.Duration(ms) * time.Millisecond)

			rf.mu.Lock()
			if prevTime == rf.lastHeartbeatTime {
				// start election
				rf.state = "CANDIDATE"
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.persist()
				rf.mu.Unlock()

				electionWon := rf.gatherVotes()

				rf.mu.Lock()
				if electionWon && rf.state == "CANDIDATE" {
					rf.state = "LEADER"
					// Initialize nextIndex to last log index + 1
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.nextIndex {
						rf.nextIndex[i] = rf.lastLogIndex() + 1
					}

					rf.broadcastEntries()
				} else {
					rf.state = "FOLLOWER"
				}

				rf.persist()
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
			}
		} else {
			rf.broadcastEntries()
			rf.mu.Unlock()
			time.Sleep(80 * time.Millisecond)
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

	// init with empty log
	rf.log = []LogEntry{}
	rf.lastHeartbeatTime = time.Now()
	rf.state = "FOLLOWER"
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.commitCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// init from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snap.Data = persister.ReadSnapshot()

	// If we have a snapshot, notify service to restore from it
	if len(rf.snap.Data) > 0 {
		rf.pendingSnap = &Snapshot{
			Index: rf.snap.Index,
			Term:  rf.snap.Term,
			Data:  rf.snap.Data,
		}
		rf.lastApplied = rf.snap.Index
		rf.commitIndex = rf.snap.Index
	}

	// background goroutines
	go rf.ticker()
	go rf.applier()

	return rf
}

// Helper methods for log indexing - all assume caller holds rf.mu

// lastLogIndex returns the logical index of the last log entry
func (rf *Raft) lastLogIndex() int {
	return rf.snap.Index + len(rf.log)
}

// lastLogTerm returns the term of the last log entry
func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.snap.Term
	}
	return rf.log[len(rf.log)-1].Term
}

// toPhysical converts a logical log index to physical slice index
// Note: globalIndex must be > snap.Index (entries at or before snap.Index are in snapshot)
func (rf *Raft) toPhysical(globalIndex int) int {
	return globalIndex - rf.snap.Index - 1
}

// snapshotFromArgs creates a Snapshot from InstallSnapshotArgs
func snapshotFromArgs(args *InstallSnapshotArgs) Snapshot {
	return Snapshot{
		Index: args.LastLogIndex,
		Term:  args.LastLogTerm,
		Data:  args.Data,
	}
}
