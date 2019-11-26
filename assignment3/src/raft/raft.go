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
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

const (
	FollowerState = iota
	CandidateState
	LeaderState
)

const debug = false

var StateNames = map[int]string{
	0: "FOLLOWER",
	1: "CANDIDATE",
	2: "LEADER",
}

type logWriter struct {
	rf *Raft
}

func (writer logWriter) Write(bytes []byte) (int, error) {
	if debug {
		return fmt.Printf("%v [%v]: %v", writer.rf.me, StateNames[writer.rf.state], string(bytes))
	}
	return 0, nil
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent state on all server
	currentTerm int
	votedFor    int
	log         []LogEntry

	//volatile state on all servers
	commitIndex int
	lastApplied int

	//volatile state on leader
	nextIndex  []int
	matchIndex []int

	// others
	state       int
	heartbeat   chan bool
	electionWin chan bool
	grantedVote chan bool
	voteCount   int
	applyCh     chan ApplyMsg

	logger log.Logger
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == LeaderState
	rf.logger.Printf("My current term: %v\n", term)
	return term, isleader
}

func (rf *Raft) getLastIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].Term
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm) // As per Figure 2 RAFT's paper
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) demote(newTerm int) {
	rf.currentTerm = newTerm
	rf.state = FollowerState
	rf.votedFor = -1
	rf.persist()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.VoteGranted = false

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		rf.logger.Printf("I don't vote for %v. I have a higher term\n", args.CandidateId)
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.demote(args.Term)
	}
	reply.Term = rf.currentTerm

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.logger.Printf("I vote for %v\n", args.CandidateId)
		rf.grantedVote <- true
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		rf.logger.Printf("I don't vote for %v. I have voted for %v\n", args.CandidateId, rf.votedFor)
	}
}

func (rf *Raft) isUpToDate(candidateIndex int, candidateTerm int) bool {
	term := rf.getLastTerm()
	if candidateTerm != term {
		return candidateTerm >= term
	}
	return candidateIndex >= rf.getLastIndex()
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		rf.logger.Printf("I won't update from %v log. I have a higher term\n", args.LeaderId)
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.demote(args.Term)
	}
	rf.logger.Printf("Received heartbeat from leader: %v\n", args.LeaderId)
	rf.heartbeat <- true
	reply.Term = rf.currentTerm

	rf.logger.Printf("My index: %v, Leader thinks my index is: %v, Leader index: %v\n", rf.getLastIndex(), args.PrevLogIndex, args.LeaderCommit)
	rf.logger.Printf("My term: %v, Leader term: %v\n", rf.currentTerm, args.Term)
	rf.logger.Printf("My log: %v, Leader log: %v\n", rf.log, args.Entries)

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > rf.getLastIndex() {
		rf.logger.Printf("Leader thinks I have a greater index, I will let him know he's wrong\n")
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}
	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		term := rf.log[args.PrevLogIndex].Term

		for reply.NextIndex = args.PrevLogIndex - 1; reply.NextIndex > 0 && rf.log[reply.NextIndex].Term == term; reply.NextIndex-- {
		}

		reply.NextIndex++
	} else {
		// If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it (§5.3)
		logsConflict := false
		restLog := make([]LogEntry, 0)
		rf.log, restLog = rf.log[:args.PrevLogIndex+1], rf.log[args.PrevLogIndex+1:]
		for i := range restLog {
			if i >= len(args.Entries) {
				break
			}
			if restLog[i].Term != args.Entries[i].Term {
				logsConflict = true
				break
			}
		}
		if logsConflict || len(restLog) < len(args.Entries) {
			// Append any new entries not already in the log
			rf.logger.Printf("There are conflicts. Log before append: %v\n", rf.log)
			rf.log = append(rf.log, args.Entries...)
			rf.logger.Printf("My Log after update: %v\n", rf.log)
		} else {
			rf.logger.Printf("There are no conflicts\n")
			rf.log = append(rf.log, restLog...)
			rf.logger.Printf("My log: %v\n", rf.log)
		}

		reply.Success = true
		reply.NextIndex = args.PrevLogIndex

		// If leaderCommit > commitIndex, set commitIndex =
		// min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit <= rf.getLastIndex() {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.getLastIndex()
			}

			go rf.commitEntries()
		}
	}

	return
}

func (rf *Raft) commitEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.logger.Printf("Committing entry: %v\n", rf.log[i])
		rf.applyCh <- ApplyMsg{i, rf.log[i].Command, false, nil}
	}

	rf.lastApplied = rf.commitIndex
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.mu.Lock()
		if rf.state == CandidateState && rf.currentTerm == args.Term {
			rf.logger.Printf("Did %v voted? %v\n", server, reply.VoteGranted)
			if reply.VoteGranted {
				rf.voteCount++
				if rf.voteCount > len(rf.peers)/2 {
					rf.electionWin <- true
				}
			} else if reply.Term > rf.currentTerm {
				rf.logger.Printf("%v has a later term: %v. Demoting to Follower\n", server, reply.Term)
				rf.demote(reply.Term)
			}
		}
		rf.mu.Unlock()
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.state != LeaderState || args.Term != rf.currentTerm {
		return ok
	}
	if reply.Term > rf.currentTerm {
		rf.logger.Printf("%v replied with a higher term, demoting myself\n", server)
		rf.demote(reply.Term)
		return ok
	}
	if reply.Success {
		rf.logger.Printf("%v successfully appended my entries\n", server)
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		rf.logger.Printf("%v is at index %v\n", server, rf.nextIndex[server])
	} else {
		rf.logger.Printf("%v failed to append my entries\n", server)
		rf.nextIndex[server] = reply.NextIndex
	}

	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3, §5.4).
	for N := rf.getLastIndex(); N > rf.commitIndex; N-- {
		count := 1

		if rf.log[N].Term == rf.currentTerm {
			for i := range rf.peers {
				if rf.matchIndex[i] >= N {
					count++
				}
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			go rf.commitEntries()
			break
		}
	}

	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := 0
	term := 0
	isLeader := rf.state == LeaderState

	if isLeader {
		index = rf.getLastIndex() + 1
		term = rf.currentTerm
		rf.logger.Printf("Log command %v in term %v\n", command, term)
		rf.log = append(rf.log, LogEntry{term, command})
		rf.persist()
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{0, nil})
	rf.state = FollowerState

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.commitIndex = len(rf.log) - 1
	rf.heartbeat = make(chan bool, 1)
	rf.electionWin = make(chan bool, 1)
	rf.grantedVote = make(chan bool, 1)
	rf.applyCh = applyCh

	rf.logger = log.Logger{}
	rf.logger.SetOutput(logWriter{rf})

	go rf.run()

	return rf
}

func (rf *Raft) run() {
	rf.logger.Printf("Start\n")
	for {
		switch rf.state {
		case FollowerState:
			select {
			case <-rf.heartbeat:
			case <-rf.grantedVote:
				rf.logger.Printf("I have voted, waiting for leader heartbeat\n")
			case <-time.After(time.Duration(rand.Int63()%200+500) * time.Millisecond):
				rf.logger.Printf("No leader is alive, promoting myself\n")
				rf.state = CandidateState
			}
		case CandidateState:
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCount = 1
			rf.persist()
			rf.mu.Unlock()
			rf.broadcastRequestVote()
			select {
			case <-time.After(700 * time.Millisecond):
			case <-rf.electionWin:
				rf.logger.Printf("I won elections\n")
				rf.mu.Lock()
				rf.state = LeaderState
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := range rf.peers {
					rf.nextIndex[i] = rf.getLastIndex() + 1
					rf.matchIndex[i] = 0
				}
				rf.mu.Unlock()
			case <-rf.heartbeat:
				rf.demote(rf.currentTerm)
			}
		case LeaderState:
			rf.logger.Printf("Sending my entries\n")
			rf.broadcastAppendEntries()
			time.Sleep(time.Millisecond * 300)
		}
	}
}

func (rf *Raft) broadcastRequestVote() {
	msg := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.getLastIndex(),
		rf.getLastTerm(),
	}
	for i := range rf.peers {
		if i != rf.me && rf.state == CandidateState {
			go rf.sendRequestVote(i, msg, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) broadcastAppendEntries() {
	for i := range rf.peers {
		if i != rf.me && rf.state == LeaderState {
			msg := AppendEntriesArgs{}
			msg.Term = rf.currentTerm
			msg.LeaderId = rf.me
			msg.PrevLogIndex = rf.nextIndex[i] - 1
			msg.LeaderCommit = rf.commitIndex
			if msg.PrevLogIndex >= 0 {
				msg.PrevLogTerm = rf.log[msg.PrevLogIndex].Term
			}
			if rf.nextIndex[i] <= rf.getLastIndex() {
				msg.Entries = rf.log[rf.nextIndex[i]:]
			}
			go rf.sendAppendEntries(i, msg, &AppendEntriesReply{})
		}
	}
}
