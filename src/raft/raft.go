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
	"crypto/rand"
	"encoding/gob"
	"labrpc"
	"math/big"
	"sync"
	"time"
)

// iota is a counter used to make const work as a enum

const (
	Follower  = iota // iota = 0
	Candidate        // iota = 1
	Leader           // iota = 2
)

const (
	HeartbeatInterval = 100 * time.Millisecond // 10 Heartbeats por segundo
)

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	//Persistent state on all servers
	currentTerm int        //last term server has seen
	votedFor    int        //candidateld that received vote in current term
	log         []LogEntry //log entries

	//Volatile state on all servers
	commitIndex int //index of highest log entry known to be commited
	lastApplied int //index of highest log entry applied to state machine

	//Volatitle state on leaders
	nextIndex  []int //for each server, index of the next log entry to send to that server
	matchIndex []int //for each server, index of the highest log entry known to be replicated on server

	//Time state
	isDead          bool
	state           int         //Follower, Candidate or Leader
	electionTimer   *time.Timer //Timer for the election
	heartbeatPeriod time.Duration

	applyCh     chan ApplyMsg
	applierCond *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) killed() bool {
	rf.mu.Lock()

	defer rf.mu.Unlock()

	return rf.isDead

}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		// Error handling? For now just ignore or log
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int //candidated's term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate's last log entry
	LastLogTerm  int //term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  //current Term to candidated to update itself
	VoteGranted bool //candidated received vote or not
}

type AppendEntriesArgs struct {
	Term         int        //leader's term
	LeaderId     int        //so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of PrevLogIndex entry
	Entries      []LogEntry //log entries to store
	LeaderCommit int        //leader's commitIndex
}

type AppendEntriesReply struct {
	Term          int  //currentTerm, for leader to update itself
	Sucess        bool //true if follower contained entry matching PrevLogIndex and PrevLogTerm
	ConflictIndex int
	ConflictTerm  int
}

//Métodos de manipulação de RPCs

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	//if Candidate's Term is lower than currentTerm, vote is denied
	if args.Term < rf.currentTerm {
		return
	}

	//if Candidate's Term is higher than the currentTerm, update Term and convert to Follower
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// Check if candidate's log is at least as up-to-date as ours
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	upToDate := false
	if args.LastLogTerm > lastLogTerm {
		upToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		upToDate = true
	}

	//if not voted or voted in the same candidate, and candidate's log is up-to-date
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimer()
	}

	rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Sucess = false
	reply.Term = rf.currentTerm

	// if the term is equal or higher, peer must become a Follower and reset the timer
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm || rf.state != Follower {
		// if the term is higher, update and become a Follower
		rf.becomeFollower(args.Term)
	}

	// 2. Reply false if log doesn't contain an entry at PrevLogIndex whose term matches PrevLogTerm
	if args.PrevLogIndex >= len(rf.log) {
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.log[i].Term == reply.ConflictTerm {
				reply.ConflictIndex = i
			} else {
				break
			}
		}
		return
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	// 4. Append any new entries not already in the log
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(rf.log) {
			if rf.log[idx].Term != entry.Term {
				rf.log = rf.log[:idx]
				rf.log = append(rf.log, entry)
			}
		} else {
			rf.log = append(rf.log, entry)
		}
	}
	rf.persist()

	// 5. If LeaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNewEntryIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewEntryIndex
		}
		rf.applierCond.Broadcast()
	}

	reply.Sucess = true
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

func (rf *Raft) sendAppendEntriesRPC(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimer()

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	votes := 1
	peers := len(rf.peers)
	var once sync.Once

	rf.mu.Unlock()
	defer rf.mu.Lock()

	for i := 0; i < peers; i++ {
		if i == rf.me {
			continue
		}

		go func(server int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != Candidate || args.Term != rf.currentTerm {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
					return
				}

				if reply.VoteGranted {
					votes++
					if votes > peers/2 {
						once.Do(func() {
							if rf.state == Candidate {
								rf.becomeLeader()
							}
						})
					}
				}
			}

		}(i)

	}
}

func (rf *Raft) randomElectionTimeout() time.Duration {
	const min int64 = 400
	const rangeSize int64 = 200

	max := big.NewInt(rangeSize)

	randomInt, err := rand.Int(rand.Reader, max)

	if err != nil {
		return time.Duration(min) * time.Millisecond
	}

	return time.Duration(min+randomInt.Int64()) * time.Millisecond
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Reset(rf.randomElectionTimeout())
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
	rf.resetElectionTimer()
}

func (rf *Raft) becomeLeader() {
	if rf.state != Candidate {
		return
	}
	rf.state = Leader

	// initializes the states of the leader (last index +1)
	lastLogIndex := len(rf.log) - 1
	peersCount := len(rf.peers)

	// nextIndex is intialized to (last index +1)
	rf.nextIndex = make([]int, peersCount)

	// matchIndex is initialized to 0
	rf.matchIndex = make([]int, peersCount)

	for i := 0; i < peersCount; i++ {
		// nextIndex to be sent to each Follower is the last log of the leader +1
		rf.nextIndex[i] = lastLogIndex + 1
		//no logs are known be replicated
		rf.matchIndex[i] = 0
	}

	// initializes immeddiatly the heartbeats after being elected
	go rf.sendHeartbeatsLoop()
}

func (rf *Raft) sendHeartbeatsLoop() {
	rf.sendAppendEntries()

	for !rf.killed() {
		time.Sleep(HeartbeatInterval)

		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		rf.sendAppendEntries()
	}
}

func (rf *Raft) sendAppendEntries() {
	rf.mu.Lock()

	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	currentTerm := rf.currentTerm
	leaderId := rf.me

	rf.mu.Unlock()

	peers := len(rf.peers)
	for i := 0; i < peers; i++ {
		if i == rf.me {
			continue
		}

		// Calculate PrevLogIndex and PrevLogTerm
		nextIdx := rf.nextIndex[i]
		prevLogIndex := nextIdx - 1
		prevLogTerm := 0
		if prevLogIndex >= 0 {
			prevLogTerm = rf.log[prevLogIndex].Term
		}

		// Prepare entries to send
		entries := make([]LogEntry, 0)
		if nextIdx < len(rf.log) {
			entries = append(entries, rf.log[nextIdx:]...)
		}

		args := AppendEntriesArgs{
			Term:         currentTerm,
			LeaderId:     leaderId,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}

		go func(server int) {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntriesRPC(server, &args, &reply)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// verify if the answer is still meaningful to currentTerm
				if args.Term != rf.currentTerm || rf.state != Leader {
					return
				}

				// if follower has a higher Term, the Leader has to become a follower
				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
					return
				}

				if reply.Sucess {
					// Update nextIndex and matchIndex
					newNextIndex := args.PrevLogIndex + len(args.Entries) + 1
					newMatchIndex := args.PrevLogIndex + len(args.Entries)

					if newNextIndex > rf.nextIndex[server] {
						rf.nextIndex[server] = newNextIndex
					}
					if newMatchIndex > rf.matchIndex[server] {
						rf.matchIndex[server] = newMatchIndex
					}

					// Update commitIndex
					// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
					// and log[N].term == currentTerm: set commitIndex = N
					for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
						count := 1 // Count self
						for j := 0; j < peers; j++ {
							if j != rf.me && rf.matchIndex[j] >= N {
								count++
							}
						}
						if count > peers/2 && rf.log[N].Term == rf.currentTerm {
							rf.commitIndex = N
							rf.applierCond.Broadcast()
							break
						}
					}

				} else {
					// Ignore stale conflict responses
					if args.PrevLogIndex != rf.nextIndex[server]-1 {
						return
					}

					// Fast backup optimization
					if reply.ConflictTerm == -1 {
						rf.nextIndex[server] = reply.ConflictIndex
					} else {
						// Find the last entry in leader's log with ConflictTerm
						lastEntryWithConflictTerm := -1
						for i := len(rf.log) - 1; i >= 0; i-- {
							if rf.log[i].Term == reply.ConflictTerm {
								lastEntryWithConflictTerm = i
								break
							}
						}

						if lastEntryWithConflictTerm != -1 {
							rf.nextIndex[server] = lastEntryWithConflictTerm + 1
						} else {
							rf.nextIndex[server] = reply.ConflictIndex
						}
					}

					// Ensure nextIndex doesn't go below 1
					if rf.nextIndex[server] < 1 {
						rf.nextIndex[server] = 1
					}
				}
			}
		}(i)
	}

}

func (rf *Raft) ticker() {
	// the Goroutine is finished if the server is dead
	for !rf.killed() {
		// wait for the election timer to expire
		<-rf.electionTimer.C

		rf.mu.Lock()
		// if the timert has expired and we are not the leader, a new election is initialized
		if rf.state != Leader {
			rf.startElection()
		}
		rf.mu.Unlock()

	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	index := len(rf.log)
	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: term, Index: index, Command: command})
	rf.persist()

	return index, term, true
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.isDead = true
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

	// 2A initialation (Initial state)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.log = []LogEntry{{Term: 0, Index: 0}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.heartbeatPeriod = HeartbeatInterval

	// initializes from persisted state before a crash
	rf.readPersist(persister.ReadRaftState())

	// initializes the election timer
	rf.electionTimer = time.NewTimer(rf.randomElectionTimeout())

	// main Goroutine of Raft to manage the timer of election
	go rf.ticker()

	rf.applyCh = applyCh
	rf.applierCond = sync.NewCond(&rf.mu)
	go rf.applier()

	return rf
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applierCond.Wait()
		}

		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied

		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.log[lastApplied+1:commitIndex+1])

		rf.lastApplied = commitIndex
		rf.mu.Unlock()

		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				Command: entry.Command,
				Index:   entry.Index,
			}
		}
	}
}
