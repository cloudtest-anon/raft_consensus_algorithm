package raft

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for Assignment2; only used in Assignment3
	Snapshot    []byte // ignore for Assignment2; only used in Assignment3
}

type LogStruct struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu          sync.Mutex
	peers       []*labrpc.ClientEnd
	log         []LogStruct
	persister   *Persister
	me          int
	currentTerm int
	votedFor    int
	timerFlag   bool
	isLeader    bool
	isKilled    bool
	commitCh    chan int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.isLeader
	rf.mu.Unlock()
	return term, isleader
}

// saving Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.isLeader)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	d.Decode(&rf.isLeader)
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AsyncRequestVoteReply struct {
	Ok    bool
	Id    int
	Reply RequestVoteReply
}

type AppendEntriesArg struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogStruct
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	LogLen  int
}

type AsyncAppendEntriesReply struct {
	Ok    bool
	Id    int
	Reply AppendEntriesReply
}

// RequestVote RPC implementation
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	cT := rf.currentTerm
	vF := rf.votedFor

	myLastTerm := rf.log[len(rf.log)-1].Term
	myLastIndex := len(rf.log) - 1

	rf.mu.Unlock()
	reply.Term = cT

	// Reply false if term < currentTerm
	if args.Term < cT {
		reply.VoteGranted = false
		return
	}

	rf.mu.Lock()

	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if (vF == -1 || vF == args.CandidateID || args.Term > cT) && ((myLastTerm == args.LastLogTerm) && (myLastIndex <= args.LastLogIndex) || myLastTerm < args.LastLogTerm) {
		reply.VoteGranted = true
		rf.timerFlag = false

		rf.votedFor = args.CandidateID
		rf.currentTerm = args.Term
		rf.persist()

	} else {
		reply.VoteGranted = false

		rf.currentTerm = args.Term
		rf.persist()
	}
	// give up leadership after receiving a request vote RPC from a candidate with a greater term
	if rf.isLeader && args.Term > cT {
		rf.isLeader = false
		rf.persist()
	}

	rf.mu.Unlock()
	reply.Term = cT
}

// AppendEntries RPC implementation
func (rf *Raft) AppendEntries(args AppendEntriesArg, reply *AppendEntriesReply) {
	rf.mu.Lock()
	cT := rf.currentTerm
	logLen := len(rf.log) - 1

	rf.mu.Unlock()
	reply.Term = cT

	if cT > args.Term { // Reply false if term < currentTerm and logs are atleast as upto date as receiver's
		reply.Success = false
		return
	} else {
		rf.mu.Lock()
		rf.timerFlag = false
		rf.isLeader = false
		rf.currentTerm = args.Term
		rf.persist()
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	if logLen < args.PrevLogIndex { // Reply false if log doesn’t contain an entry at prevLogIndex
		reply.Success = false

	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { // Reply false if log contains an entry at prevLogIndex with Term that does not match with prevLogTerm
		reply.Success = false
	} else {
		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.log = append(rf.log, args.Entries...)
		reply.Success = true
		rf.persist()
	}
	reply.LogLen = len(rf.log)

	if (args.LeaderCommit > rf.commitIndex) && reply.Success { // update commite index
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		if rf.commitIndex > rf.lastApplied {
			rf.commitCh <- rf.commitIndex
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArg, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.isLeader
	if isLeader {
		rf.log = append(rf.log, LogStruct{rf.currentTerm, command})
		// fmt.Println("me: ", rf.me, " logs: ", rf.log)
		rf.persist()
		index = len(rf.log) - 1
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	rf.mu.Lock()
	rf.isKilled = true
	rf.mu.Unlock()
}

// Creating a raft server
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.isLeader = false // *** changed it from true to false
	rf.isKilled = false
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogStruct{rf.currentTerm, -1}) // storing a dummy value on index since raft implementation has all values starting from index 1
	rf.readPersist(persister.ReadRaftState())
	rf.commitCh = make(chan int)

	var makeCandidate = make(chan bool)
	var makeLeader = make(chan bool)
	var resetTimerFlag = make(chan bool)

	go rf.stateMachine(applyCh)                                            // this thread applies commands to state machine when a commitIndex is received via channel
	rf.commitCh <- 0                                                       // fixing the dummy value on the 0th index of the committed values
	go rf.stateHandler(applyCh, makeCandidate, makeLeader, resetTimerFlag) // this thread allows raft to switch between candidate and leader state
	go rf.electionTimeout(makeCandidate, resetTimerFlag)                   // this thread keeps a track of election timeouts

	rf.mu.Lock()
	if rf.isLeader {
		makeLeader <- true
	}
	rf.mu.Unlock()

	return rf
}

// It starts a for loop that sets the case to either a candidate state or a Leader state and calls their respective go routine i.e send requestVotes or send appendEntries
func (rf *Raft) stateHandler(applyCh chan ApplyMsg, makeCandidate chan bool, makeLeader chan bool, resetTimerFlag chan bool) {

	for {
		select {
		case <-makeCandidate:
			go rf.sendRequestVoteToAll(makeLeader, resetTimerFlag)
		case <-makeLeader:
			rf.mu.Lock()
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for i := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.log)
			}
			rf.mu.Unlock()
			go rf.appendEntries()

		}

	}
}

// this function keeps track of election timeout while running in the background as long as the raft is alive. In case the raft does not hear from a leader within timeout or an election fails, it assumes a candidate position
func (rf *Raft) electionTimeout(makeCandidate chan bool, resetTimerFlag chan bool) {
	for {
		rf.mu.Lock()
		rf.timerFlag = true // if this flag remains true after timeout then a candidate phase begins for this server
		rf.mu.Unlock()
		rnd := rand.Intn(300) + 300 //rand time b/w 300,600ms
		time.Sleep(time.Millisecond * time.Duration(rnd))
		rf.mu.Lock()
		flag := rf.timerFlag
		isLeader := rf.isLeader
		if rf.isKilled {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		if flag && !isLeader {
			makeCandidate <- true // initiates candidate phase
			<-resetTimerFlag
		}
	}
}

// function to asynchronously grab votes from servers through RPC
func (rf *Raft) sendAsyncRequestVote(chanAsyncReply chan AsyncRequestVoteReply, serverID int, args RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.sendRequestVote(serverID, args, reply) //RPC for requestVote
	chanAsyncReply <- AsyncRequestVoteReply{ok, serverID, RequestVoteReply{reply.Term, reply.VoteGranted}}
}

// this function sends vote requests to all rafts and allows the local raft to assume leader position if votes from a majority are received
func (rf *Raft) sendRequestVoteToAll(makeLeader chan bool, resetTimerFlag chan bool) {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()
	peersCount := len(rf.peers)
	args := RequestVoteArgs{rf.currentTerm, rf.me, len(rf.log) - 1, rf.log[len(rf.log)-1].Term}

	rf.mu.Unlock()
	resetTimerFlag <- true

	count := 1
	toFollower := false
	chanAsyncReply := make(chan AsyncRequestVoteReply, peersCount-1) // channel used to grab RPC responses

	// Run RequestVote RPC for all servers asynchronously
	for x := 0; x < peersCount; x++ {
		if x != args.CandidateID {
			go rf.sendAsyncRequestVote(chanAsyncReply, x, args, &RequestVoteReply{})
		}
	}

	msgCounts := 1
	for {
		asyncReply := <-chanAsyncReply // getting responses from channel
		msgCounts++
		ok := asyncReply.Ok
		reply := asyncReply.Reply
		if ok {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm || rf.votedFor != rf.me {
				rf.currentTerm = reply.Term
				rf.mu.Unlock()
				toFollower = true
				break
			}
			rf.mu.Unlock()
			if reply.VoteGranted {
				count += 1
				if !toFollower && count > peersCount/2 {
					rf.mu.Lock()
					rf.timerFlag = false
					rf.isLeader = true
					rf.persist()
					rf.mu.Unlock()
					makeLeader <- true
					break
				}
			}
		}
		// checking if all responses have been received
		if msgCounts == peersCount {
			break
		}
	}
}

// function to asynchronously send AppendEntries RPC
func (rf *Raft) sendAsyncAppendEntries(chanAsyncReply chan AsyncAppendEntriesReply, serverID int, args AppendEntriesArg, reply *AppendEntriesReply) {
	ok := rf.sendAppendEntries(serverID, args, reply)
	chanAsyncReply <- AsyncAppendEntriesReply{ok, serverID, AppendEntriesReply{reply.Term, reply.Success, reply.LogLen}}
}

// This function runs when server is elected as a leader. It waits for replies from followers to its AppendEntries RPC that are received through a channel.
// It starts two go routines. One sends AppendEntries RPC (heartbeats) and one is used to find commit Index.
func (rf *Raft) appendEntries() {

	rf.mu.Lock()
	peersCount := len(rf.peers)
	rf.mu.Unlock()
	go rf.commitLog()
	chanAsyncReply := make(chan AsyncAppendEntriesReply, peersCount) // channel used to grab RPC responses
	go rf.heartbeats(chanAsyncReply)

	msgCounts := 1
	for {
		asyncReply := <-chanAsyncReply // getting responses from channel
		msgCounts++
		ok := asyncReply.Ok
		reply := asyncReply.Reply
		serverID := asyncReply.Id
		if ok {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm || !rf.isLeader { // if term received from a follower is greater than current term then give up leadership
				rf.currentTerm = reply.Term
				rf.isLeader = false
				rf.persist()
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			if reply.Success { // if logs are successfully replicated then update nextIndex and matchIndex for that particular follower
				rf.mu.Lock()
				rf.nextIndex[serverID] = reply.LogLen
				rf.matchIndex[serverID] = rf.nextIndex[serverID] - 1
				rf.mu.Unlock()

			} else {
				rf.mu.Lock()
				rf.nextIndex[serverID] -= 1
				rf.mu.Unlock()
			}

		}
		rf.mu.Lock()
		if !rf.isLeader || rf.isKilled { // end this thread when the leader status of this server is dissolved or when the server is killed
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		if msgCounts == peersCount {
			msgCounts = 1
		}
	}
}

// It runs AppendEntries RPC for all servers asynchronously 10 times per sec and sends their response to the parent go routine via a channel
func (rf *Raft) heartbeats(chanAsyncReply chan AsyncAppendEntriesReply) {

	rf.mu.Lock()
	peersCount := len(rf.peers)
	rf.mu.Unlock()

	for {
		for x := 0; x < peersCount; x++ {

			rf.mu.Lock()

			if x != rf.me && rf.isLeader {
				prevLogTerm := rf.log[rf.nextIndex[x]-1].Term
				prevLogIndex := rf.nextIndex[x] - 1

				entries := rf.log[rf.nextIndex[x]:]
				entriesCopied := make([]LogStruct, len(entries))
				copy(entriesCopied, entries) // deep copy

				args := AppendEntriesArg{rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, entriesCopied, rf.commitIndex}

				go rf.sendAsyncAppendEntries(chanAsyncReply, x, args, &AppendEntriesReply{})
			}

			if !rf.isLeader || rf.isKilled { // end this thread when the leader status of this server is dissolved or when the server is killed
				rf.mu.Unlock()
				return
			}

			rf.mu.Unlock()
		}
		time.Sleep(time.Second / 10) // 10 heartbeats per sec

	}
}

// It calculates the commitIndex and passes that value to the state machine via a channel
func (rf *Raft) commitLog() {
	for {
		time.Sleep(time.Second / 10)

		rf.mu.Lock()
		length := len(rf.log)
		peerCount := len(rf.peers)

		// cT := rf.currentTerm
		if rf.isKilled || !rf.isLeader { // end this thread when the leader status of this server is dissolved or when the server is killed
			rf.mu.Unlock()
			break
		}
		commitUpdated := false

		// logTerm := rf.commitIndex
		// fmt.Println("not running ", rf.commitIndex+1, length)
		for N := rf.commitIndex + 1; N < length; N++ {
			// fmt.Println("running ", rf.commitIndex+1, length)
			count := 1
			for i := range rf.peers {
				if rf.matchIndex[i] >= N {
					count++
				}
			}
			// logTerm = rf.log[N].Term
			if count > peerCount/2 {
				rf.commitIndex = N
				commitUpdated = true
			}
		}
		// fmt.Println("ct: ", cT, " logterm: ", logTerm)
		// fmt.Println("commit Updated: ", commitUpdated, " commitIndex: ", rf.commitIndex, " me: ", rf.me)
		// apply to state machine
		if commitUpdated { // checking safety by comparing cT == logTerm
			rf.commitCh <- rf.commitIndex
		}
		rf.mu.Unlock()
	}
}

// It waits for the commit Index to be received via a channel and applies all the commands till that Index to the state machine
func (rf *Raft) stateMachine(applyCh chan ApplyMsg) {
	for {
		commitIndex := <-rf.commitCh

		rf.mu.Lock()
		lastApplied := rf.lastApplied
		entriesToApply := rf.log[rf.lastApplied+1 : commitIndex+1]
		entriesToApplyCopied := make([]LogStruct, len(entriesToApply))
		copy(entriesToApplyCopied, entriesToApply)
		rf.lastApplied = commitIndex
		// fmt.Println(entriesToApplyCopied)
		for i, val := range entriesToApplyCopied {
			applyCh <- ApplyMsg{lastApplied + i, val.Command, false, nil}
		}

		if rf.isKilled {
			break
		}

		rf.mu.Unlock()
	}
}

// find min of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
