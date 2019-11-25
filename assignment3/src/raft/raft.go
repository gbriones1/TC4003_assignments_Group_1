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
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

const (
	StateFollower = iota
	StateCandidate
	StateLeader
)

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
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == StateLeader
	fmt.Printf("%v: My current term: %v, am I leader?: %v\n", rf.me, term, isleader)
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
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	// Do not grant vote if candidate term < currentTerm
	if args.Term < rf.currentTerm {
		fmt.Printf("%v: I don't vote for %v. I have a higher term\n", rf.me, args.CandidateId)
		return
	}
	// Become follower if candidate term > currentTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = args.Term
		rf.state = StateFollower
		rf.votedFor = -1
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		fmt.Printf("%v: I vote for %v\n", rf.me, args.CandidateId)
		rf.grantedVote <- true
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		fmt.Printf("%v: I don't vote for %v. I have voted for %v\n", rf.me, args.CandidateId, rf.votedFor)
	}
	rf.persist()
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

	fmt.Printf("%v: Received heartbeat from leader: %v\n", rf.me, args.LeaderId)
	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = StateFollower
		rf.votedFor = -1
	}

	rf.heartbeat <- true
	reply.Term = rf.currentTerm
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
		if rf.state == StateCandidate && rf.currentTerm == args.Term {
			fmt.Printf("%v: Did %v voted? %v\n", rf.me, server, reply.VoteGranted)
			if reply.VoteGranted {
				rf.voteCount++
				if rf.voteCount > len(rf.peers)/2 {
					rf.electionWin <- true
				}
			} else if reply.Term > rf.currentTerm {
				fmt.Printf("%v: %v has a later term: %v. Demoting to Follower\n", rf.me, server, reply.Term)
				rf.currentTerm = reply.Term
				rf.state = StateFollower
				rf.votedFor = -1
				rf.persist()
			}
		}
		rf.mu.Unlock()
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	isLeader := rf.state == StateLeader

	if isLeader {
		index = rf.getLastIndex() + 1
		term = rf.currentTerm
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
	rf.state = StateFollower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// rf.commitIndex = len(rf.log) - 1
	rf.heartbeat = make(chan bool, 1)
	rf.electionWin = make(chan bool, 1)
	rf.grantedVote = make(chan bool, 1)

	go rf.run()

	return rf
}

func (rf *Raft) run() {
	for {
		switch rf.state {
		case StateFollower:
			select {
			// If a leader is alive
			case <-rf.heartbeat:
			// If I have voted for a candidate
			case <-rf.grantedVote:
				fmt.Printf("%v: I have voted, waiting for leader heartbeat\n", rf.me)
			// If no leader is alive or no candidates, then timeout is reached
			case <-time.After(time.Duration(rand.Int63()%200+500) * time.Millisecond):
				fmt.Printf("%v: No leader alive, promoting myself\n", rf.me)
				rf.mu.Lock()
				rf.state = StateCandidate
				rf.mu.Unlock()
			}
		case StateCandidate:
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCount = 1
			rf.persist()
			rf.mu.Unlock()
			rf.broadcastRequestVote()
			select {
			// Wait for responses
			case <-time.After(700 * time.Millisecond):
			// Wait to be elected
			case <-rf.electionWin:
				fmt.Printf("%v: I won\n", rf.me)
				rf.mu.Lock()
				rf.state = StateLeader
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := range rf.peers {
					rf.nextIndex[i] = rf.getLastIndex() + 1
					rf.matchIndex[i] = 0
				}
				rf.mu.Unlock()
			// If somebody else becomes a leader
			case <-rf.heartbeat:
				rf.mu.Lock()
				rf.state = StateFollower
				rf.mu.Unlock()
			}
			fmt.Printf("%v: Ended election\n", rf.me)
		case StateLeader:
			fmt.Printf("%v: As a leader, send my entries\n", rf.me)
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
		if i != rf.me && rf.state == StateCandidate {
			go rf.sendRequestVote(i, msg, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) broadcastAppendEntries() {
	for i := range rf.peers {
		if i != rf.me && rf.state == StateLeader {
			msg := AppendEntriesArgs{
				rf.currentTerm,
				rf.me,
				rf.nextIndex[i] - 1,
				rf.log[rf.nextIndex[i]-1].Term,
				rf.log,
				rf.commitIndex,
			}
			go rf.sendAppendEntries(i, msg, &AppendEntriesReply{})
		}
	}
}
