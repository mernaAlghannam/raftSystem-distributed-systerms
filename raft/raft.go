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
	"cs350/labgob"
	"cs350/labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

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

// Raft log entry
type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int
	dead      int32

	applyCh chan string

	currentTerm   int
	voteFor       int
	logs          []LogEntry
	SnapshotIndex int
	SnapshotTerm  int

	commitIndex     int
	lastApplied     int
	state           string
	electionTimeout time.Time

	nextIndex  []int
	matchIndex []int

	voteCount int
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan<- ApplyMsg) *Raft {
	// rf := &Raft{}
	rf := &Raft{}

	rf.applyCh = make(chan string)

	rf.SnapshotIndex = 0
	rf.SnapshotTerm = 0

	rf.electionTimeout = time.Now().Add(time.Duration(rand.Intn(500)) * time.Millisecond)

	rf.nextIndex = nil
	rf.matchIndex = nil

	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.state = "F"

	rf.logs = []LogEntry{}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.voteCount = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// • Upon election: send initial empty AppendEntries RPCs
	// (heartbeat) to each server; repeat during idle periods to
	// prevent election timeouts (§5.2)

	rf.commitIndex = rf.SnapshotIndex
	rf.lastApplied = rf.SnapshotIndex

	go rf.ticker()

	go rf.applier(applyCh)

	defer func() {

		if rf.SnapshotIndex != 0 {
			data := rf.persister.ReadSnapshot()
			if len(data) != 0 {
				applyMsg := ApplyMsg{
					CommandValid:  false,
					SnapshotValid: true,
					Snapshot:      data,
					SnapshotIndex: rf.SnapshotIndex,
					SnapshotTerm:  rf.SnapshotTerm,
				}

				applyCh <- applyMsg
			}
		} else {
			applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      0,
				CommandIndex: 0,
			}
		}

	}()

	// defer func() {
	// 	if rf.SnapshotIndex != 0 {
	// 		data := rf.persister.ReadSnapshot()
	// 		if len(data) != 0 {
	// 			applyMsg := ApplyMsg{
	// 				CommandValid:  false,
	// 				SnapshotValid: true,
	// 				Snapshot:      data,
	// 				SnapshotIndex: rf.SnapshotIndex,
	// 				SnapshotTerm:  rf.SnapshotTerm,
	// 			}

	// 			applyCh <- applyMsg
	// 		}
	// 	}
	// }()

	return rf
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.SnapshotIndex {
		return
	}

	rf.SnapshotTerm = rf.logs[index-rf.SnapshotIndex-1].Term

	rf.logs = rf.logs[index-rf.SnapshotIndex:]

	rf.SnapshotIndex = index

	var data []byte
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.voteFor) != nil ||
		e.Encode(rf.logs) != nil ||
		e.Encode(rf.SnapshotIndex) != nil ||
		e.Encode(rf.SnapshotTerm) != nil {
		// data = nil
		// return
	} else {
		data = w.Bytes()
		rf.persister.SaveStateAndSnapshot(data, snapshot)
	}

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm || args.LeaderId == rf.me || args.LastIncludedIndex <= rf.SnapshotIndex {
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = "F"
		rf.currentTerm = args.Term
		rf.voteFor = -1

		rf.persist()
	}

	rf.SnapshotIndex = args.LastIncludedIndex
	rf.SnapshotTerm = args.LastIncludedTerm
	rf.commitIndex = rf.SnapshotIndex
	rf.lastApplied = rf.SnapshotIndex

	for i := range rf.logs {
		if rf.logs[i].Index == args.LastIncludedIndex && rf.logs[i].Term == args.LastIncludedTerm {
			rf.logs = rf.logs[i+1:]
			var data []byte
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			if e.Encode(rf.currentTerm) != nil ||
				e.Encode(rf.voteFor) != nil ||
				e.Encode(rf.logs) != nil ||
				e.Encode(rf.SnapshotIndex) != nil ||
				e.Encode(rf.SnapshotTerm) != nil {
				// data = nil
				// return
			} else {
				data = w.Bytes()
				rf.persister.SaveStateAndSnapshot(data, args.Data)
			}

			select {
			case rf.applyCh <- "snapshot":
			default:
			}
			return
		}
	}
	rf.logs = make([]LogEntry, 0)
	var data []byte
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.voteFor) != nil ||
		e.Encode(rf.logs) != nil ||
		e.Encode(rf.SnapshotIndex) != nil ||
		e.Encode(rf.SnapshotTerm) != nil {
		// data = nil
		// return
	} else {
		data = w.Bytes()
		rf.persister.SaveStateAndSnapshot(data, args.Data)
	}

	select {
	case rf.applyCh <- "snapshot":
	default:
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	isleader = rf.state == "L"
	term = rf.currentTerm
	rf.mu.Unlock()

	return term, isleader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// candidate wins election, comes to power, with mutex held
func (rf *Raft) setNewLeader() {
	rf.state = "L"

	lastLogIndex := rf.SnapshotIndex
	lenLog := len(rf.logs)
	if lenLog > 0 {
		lastLogIndex = rf.logs[lenLog-1].Index
	}

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = lastLogIndex + 1
	}

	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = lastLogIndex

	go rf.heartbeats(rf.currentTerm)
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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm || args.CandidateId == rf.me {
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = "F"
		rf.currentTerm = args.Term
		rf.voteFor = -1

		rf.persist()
	}

	var isLogUpToDate bool
	myLastLogIndex := rf.SnapshotIndex
	myLastLogTerm := rf.SnapshotTerm
	if len(rf.logs) > 0 {
		myLastLogIndex = rf.logs[len(rf.logs)-1].Index
		myLastLogTerm = rf.logs[len(rf.logs)-1].Term
	}

	if myLastLogTerm != args.LastLogTerm {
		isLogUpToDate = myLastLogTerm > args.LastLogTerm
	} else {

		isLogUpToDate = myLastLogIndex > args.LastLogIndex
	}

	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) &&
		!isLogUpToDate {
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId

		rf.persist()
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// The ticker go routine starts a new election if this peer hasn't received heartbeats recently.
func (rf *Raft) ticker() {

	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// • On conversion to candidate, start election:
		// • Vote for self
		// • Reset election timer
		// • Send RequestVote RPCs to all other servers

		rf.mu.Lock()

		if time.Until(rf.electionTimeout).Milliseconds() > time.Until(time.Now()).Milliseconds() {
			rf.mu.Unlock()
			// } else if rf.state == "L" {
			// 	rf.electionTimeout = time.Now().Add(time.Duration(rand.Intn(1000)+500) * time.Millisecond)
			// 	rf.mu.Unlock()
		} else {
			rf.state = "C"
			rf.currentTerm += 1
			rf.voteFor = rf.me
			rf.persist()

			rf.electionTimeout = time.Now().Add(time.Duration(rand.Intn(1000)+500) * time.Millisecond)

			rf.mu.Unlock()

			rf.startElection(rf.currentTerm)
		}
		time.Sleep(10 * time.Millisecond)
	}

}

func (rf *Raft) startElection(term int) {
	// rf.voteCount = 1
	// for server := range rf.peers {
	// 	if server == rf.me || rf.currentTerm != term {
	// 		continue
	// 	}
	// 	go rf.countVote(term, server)
	// }
	rf.mu.Lock()
	var finalLogIndex int
	var finalLogTerm int

	finalLogIndex = rf.SnapshotIndex
	finalLogTerm = rf.SnapshotTerm

	if len(rf.logs) > 0 {
		finalLogIndex = rf.logs[len(rf.logs)-1].Index
		finalLogTerm = rf.logs[len(rf.logs)-1].Term
	}

	args := &RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = finalLogIndex
	args.LastLogTerm = finalLogTerm
	rf.mu.Unlock()

	rf.voteCount = 1
	done := false
	for server := range rf.peers {
		if done {
			break
		}
		if server == rf.me {
			continue
		}
		done = rf.countVote(term, server, args)
	}

}

// The requestVote go routine sends RequestVote RPC to one peer and deal with reply
func (rf *Raft) countVote(term int, server int, args *RequestVoteArgs) bool {
	rf.mu.Lock()
	if rf.state != "C" || rf.currentTerm != term {
		rf.mu.Unlock()
		return true
	}
	rf.mu.Unlock()

	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if rf.state != "C" || args.Term != rf.currentTerm {
			// rf.mu.Unlock()
			return true
		}

		if reply.Term > rf.currentTerm {
			rf.state = "F"
			rf.currentTerm = reply.Term
			rf.voteFor = -1

			rf.persist()
			return true
		}

		if reply.VoteGranted {
			rf.voteCount++
			if rf.voteCount >= len(rf.peers)/2+1 {
				if rf.state == "C" && rf.currentTerm == args.Term && !rf.killed() {
					rf.setNewLeader()
					return true

				}
			}
		}
	}
	// rf.mu.Unlock()

	return false
}

// // the service using Raft (e.g. a k/v server) wants to start
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

	var index int
	var term int

	// Your code here (2B).
	rf.mu.Lock()
	// defer rf.mu.Unlock()
	term = rf.currentTerm

	if rf.state != "L" {
		rf.mu.Unlock()
		return index, term, false
	}

	index = rf.nextIndex[rf.me]

	rf.logs = append(rf.logs, LogEntry{term, index, command})
	rf.nextIndex[rf.me]++
	rf.matchIndex[rf.me] = index

	rf.persist()
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me {

			rf.sendheartbeats(i, term)
			// {
			// 	break
			// }
		}
	}

	return index, term, true
}

func (rf *Raft) heartbeats(term int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != "L" || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		for i := range rf.peers {
			if i != rf.me {
				go rf.sendheartbeats(i, term)
			}
		}
		rf.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) applier(applyCh chan<- ApplyMsg) {
	var typeOfApply string

	for !rf.killed() {
		select {
		case typeOfApply = <-rf.applyCh:

			if typeOfApply == "commit" {
				rf.mu.Lock()

				for rf.commitIndex > rf.lastApplied {
					i := rf.lastApplied + 1
					commitLog := rf.logs[rf.lastApplied-rf.SnapshotIndex].Command
					rf.lastApplied++
					rf.mu.Unlock()
					applyCh <- ApplyMsg{
						CommandValid: true,
						Command:      commitLog,
						CommandIndex: i,
					}
					rf.mu.Lock()
					// rf.lastApplied = i
				}

				rf.mu.Unlock()

			} else {
				rf.mu.Lock()
				data := rf.persister.ReadSnapshot()
				if rf.SnapshotIndex == 0 || len(data) == 0 {
					rf.mu.Unlock()
					continue
				}
				applyMsg := ApplyMsg{
					CommandValid:  false,
					SnapshotValid: true,
					Snapshot:      data,
					SnapshotIndex: rf.SnapshotIndex,
					SnapshotTerm:  rf.SnapshotTerm,
				}
				rf.mu.Unlock()

				applyCh <- applyMsg
			}
		}
	}
}

func (rf *Raft) persist() {
	// Your code here (2C).

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.voteFor) != nil ||
		e.Encode(rf.logs) != nil ||
		e.Encode(rf.SnapshotIndex) != nil ||
		e.Encode(rf.SnapshotTerm) != nil {
		panic("failed to to add persist")
	}
	data := w.Bytes()

	rf.persister.SaveRaftState(data)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
	// Your code here (2C).
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// if d.Decode(&rf.currentTerm) != nil ||
	// 	d.Decode(&rf.votedFor) != nil || d.Decode(&rf.logs) != nil {
	// 	panic("failed to decode raft persistent state")
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, voteFor, lastIncludedIndex, lastIncludedTerm int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		return
	}
	rf.currentTerm = currentTerm
	rf.voteFor = voteFor
	rf.logs = logs
	rf.SnapshotIndex = lastIncludedIndex
	rf.SnapshotTerm = lastIncludedTerm
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm || args.LeaderId == rf.me {

		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm || (rf.state == "C" && args.Term >= rf.currentTerm) {
		rf.state = "F"
		rf.currentTerm = args.Term
		rf.voteFor = -1

		rf.persist()
	}

	rf.electionTimeout = time.Now().Add(time.Duration(rand.Intn(1000)+500) * time.Millisecond)

	lastLogIndex := rf.SnapshotIndex
	if len(rf.logs) > 0 {
		lastLogIndex = rf.logs[len(rf.logs)-1].Index
	}

	if lastLogIndex < args.PrevLogIndex {
		reply.ConflictIndex = lastLogIndex + 1
		reply.Success = false
		return
	}

	var prevLogTerm int
	if args.PrevLogIndex == rf.SnapshotIndex {
		prevLogTerm = rf.SnapshotTerm
	} else if args.PrevLogIndex > rf.SnapshotIndex {
		prevLogTerm = rf.logs[args.PrevLogIndex-rf.SnapshotIndex-1].Term
	} else {

		args.PrevLogIndex = rf.SnapshotIndex
		prevLogTerm = rf.SnapshotTerm
		ie := 0
		for ie = range args.Entries {
			if args.Entries[ie].Index == rf.SnapshotIndex && args.Entries[ie].Term == rf.SnapshotTerm {
				break
			}
		}

		if len(args.Entries) == 0 {
			args.Entries = make([]LogEntry, 0)
		} else if args.Entries[ie].Index != rf.SnapshotIndex || args.Entries[ie].Term != rf.SnapshotTerm {
			// if args.Entries == args.Entries[ie+1:]{

			args.Entries = make([]LogEntry, 0)
		} else if args.Entries[ie].Index == rf.SnapshotIndex && args.Entries[ie].Term == rf.SnapshotTerm {
			args.Entries = args.Entries[ie+1:]
		}

	}

	if prevLogTerm != args.PrevLogTerm {
		reply.ConflictTerm = prevLogTerm
		for i := args.PrevLogIndex - rf.SnapshotIndex - 1; i > -1; i-- {
			reply.ConflictIndex = rf.logs[i].Index
			if rf.logs[i].Term != prevLogTerm {
				break
			}
		}
		reply.Success = false
		return
	}

	reply.Success = true

	if len(args.Entries) > 0 {

		givenEntries := rf.logs[args.PrevLogIndex-rf.SnapshotIndex:]
		var i int
		for i = 0; i < min(len(givenEntries), len(args.Entries)); i++ {
			if givenEntries[i].Term != args.Entries[i].Term {
				rf.logs = rf.logs[:args.PrevLogIndex-rf.SnapshotIndex+i]
				rf.persist()
				break
			}
		}
		if i < len(args.Entries) {
			rf.logs = append(rf.logs, args.Entries[i:]...)
			rf.persist()
		}

	}

	if args.LeaderCommit > rf.commitIndex {
		lastLogIndex := rf.SnapshotIndex
		if len(rf.logs) > 0 {
			lastLogIndex = rf.logs[len(rf.logs)-1].Index
		}
		if args.LeaderCommit < lastLogIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastLogIndex
		}
		select {
		case rf.applyCh <- "commit":
		default:
		}
	}
}

func (rf *Raft) sendheartbeats(server int, term int) {
	rf.mu.Lock()
	if rf.state != "L" || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.SnapshotTerm
	ind := prevLogIndex - rf.SnapshotIndex - 1
	if ind > -1 {
		prevLogTerm = rf.logs[ind].Term
	}

	var entries []LogEntry
	lastLogIndex := rf.SnapshotIndex
	if len(rf.logs) > 0 {
		lastLogIndex = rf.logs[len(rf.logs)-1].Index
	}
	if ni := rf.nextIndex[server]; lastLogIndex >= ni && ni > rf.SnapshotIndex {
		nEntries := rf.logs[ni-rf.SnapshotIndex-1:]
		entries = make([]LogEntry, len(nEntries))
		copy(entries, nEntries)
	}

	args := &AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = prevLogTerm
	args.Entries = entries
	args.LeaderCommit = rf.commitIndex

	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)

	if ok {

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.state != "L" {
			return
		} else if reply.Term > rf.currentTerm {
			rf.state = "F"
			rf.currentTerm = reply.Term
			rf.voteFor = -1

			rf.persist()
			return
		}

		if reply.Success {

			if args.PrevLogIndex+len(args.Entries)+1 > rf.nextIndex[server] {
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			}
			if args.PrevLogIndex+len(args.Entries) > rf.matchIndex[server] {
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			}

		} else if !reply.Success {

			li := len(rf.logs) - 1
			if reply.ConflictIndex != 0 && reply.ConflictTerm == 0 {
				rf.nextIndex[server] = reply.ConflictIndex
			} else {
				var index int
				var term int
				for li >= -1 {
					if li < 0 && len(rf.logs) > 0 {
						index = rf.logs[len(rf.logs)-1].Index
						term = rf.logs[len(rf.logs)-1].Term
					} else if li < 0 {
						index = rf.SnapshotIndex
						term = rf.SnapshotTerm
					} else {
						index = rf.logs[li].Index
						term = rf.logs[li].Term
					}

					if term == reply.ConflictTerm {
						rf.nextIndex[server] = index + 1
						break
					} else if term < reply.ConflictTerm {
						rf.nextIndex[server] = reply.ConflictIndex
						break
					} else if li < 0 {
						rf.nextIndex[server] = rf.SnapshotIndex + 1
					}
					li--
				}
			}

			if rf.nextIndex[server] <= rf.SnapshotIndex || li < 0 {
				go func(server int, term int) {
					rf.mu.Lock()
					if rf.state != "L" || rf.currentTerm != term {
						rf.mu.Unlock()
						return
					}
					args := &InstallSnapshotArgs{}
					args.Term = rf.currentTerm
					args.LeaderId = rf.me
					args.LastIncludedIndex = rf.SnapshotIndex
					args.LastIncludedTerm = rf.SnapshotTerm
					args.Data = rf.persister.ReadSnapshot()

					rf.mu.Unlock()

					reply := &InstallSnapshotReply{}
					ok := rf.sendInstallSnapshot(server, args, reply)

					if ok {

						rf.mu.Lock()
						defer rf.mu.Unlock()

						if rf.state != "L" {
							return
						}

						if reply.Term > rf.currentTerm {
							rf.state = "F"
							rf.currentTerm = reply.Term
							rf.voteFor = -1

							rf.persist()
							return
						}

						if args.LastIncludedIndex+1 > rf.nextIndex[server] {
							rf.nextIndex[server] = args.LastIncludedIndex + 1
						}
						if args.LastIncludedIndex > rf.matchIndex[server] {
							rf.matchIndex[server] = args.LastIncludedIndex
						}
					}
				}(server, term)
				return
			}

			return
		}

		if rf.state != "L" || rf.currentTerm != term {
			return
		}

		lastLogIndex := rf.SnapshotIndex
		if len(rf.logs) > 0 {
			lastLogIndex = rf.logs[len(rf.logs)-1].Index
		}
		if rf.commitIndex >= lastLogIndex {
			return
		}
		for j := rf.commitIndex + 1; j < len(rf.logs)+rf.SnapshotIndex+1; j++ {
			if rf.logs[j-rf.SnapshotIndex-1].Term == term {
				count := 1
				for i := range rf.peers {
					if i != rf.me && rf.matchIndex[i] >= j {
						count++
					}
				}
				if count >= len(rf.peers)/2+1 {
					rf.commitIndex = j
					select {
					case rf.applyCh <- "commit":
					default:
					}
				}
			}
		}
	}
}

