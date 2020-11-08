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

import "sync"
import "sync/atomic"
import "../labrpc"
import "time"
import "math/rand"

import "sort"

import "bytes"
import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type logEntry struct {
	Index   int
	Term    int
	Command interface{}
	// command for state machine
	// term when entry was received by leader (first index is 1)
}

type serverState int

const (
	CANDIDATE serverState = iota
	FOLLOWER
	LEADER
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent State
	// latest term server has seen
	curTerm int

	duration time.Duration

	lastAccess time.Time
	// candidateId that received vote in current term(or null if none)
	votedFor int

	// log entries
	logs []logEntry
	// Volatile state on all servers
	commitIndex int
	// index of highest log entry applied to state machine
	lastApplied int
	applyCh     *chan ApplyMsg

	// volatile state on leaders
	// for each server, index of the next log entry to send to that server
	nextIndex []int
	// for each server, index of highest log entry known to be replicated on server
	matchIndex []int

	state serverState
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.curTerm, (rf.state == LEADER)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	rf.mu.Lock()
	e.Encode(rf.curTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	rf.mu.Unlock()
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var curTerm int
	var votedFor int
	var logs []logEntry
	if d.Decode(&curTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		return
	} else {
		rf.mu.Lock()
		rf.curTerm = curTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.mu.Unlock()
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// candidate's term
	CandTerm int
	// candidate requesting vote
	CandidateId int
	// index of candidate's last log entry
	LastLogIndex int
	// term of candidate's last log entry
	LastLogTerm int
}

type RequestVoteReply struct {
	CurTerm     int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// sender is a candidate
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	reply.CurTerm = rf.curTerm
	// longer
	// TODO: rewrite
	// if rf logs is longer and term is greater or equal to candidate's term
	if rf.logs[len(rf.logs)-1].Term > args.LastLogTerm {
		return
	}
	// if rf logs is equal and term is greater
	if rf.logs[len(rf.logs)-1].Term == args.LastLogTerm && ((len(rf.logs) - 1) > args.LastLogIndex) {
		return
	}
	if args.CandTerm > rf.curTerm {
		rf.votedFor = args.CandidateId
		rf.state = FOLLOWER
		rf.curTerm = args.CandTerm
		reply.VoteGranted = true
		reply.CurTerm = rf.curTerm
		go func() {
			rf.persist()
		}()
	}
	// and candidate's log is at least as updated as receiver's log
	if (args.CandTerm == rf.curTerm) && ((rf.votedFor == -1) || (rf.votedFor == args.CandidateId)) {
		// change state?
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		go func() {
			rf.persist()
		}()
	}
	return
}

type AppendEntriesArgs struct {
	// Leader's term.
	LeaderTerm int
	// Leader's id so follower can redirect clients.
	LeaderId int
	// Index of log entry immediately preceding new ones.
	PrevLogIndex int
	// Term of PrevLogIndex entry.
	PrevLogTerm int
	// Log entries to store(empty for heartbeat).
	Entries []logEntry
	// Leader's commitIndex.
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	// Current term of the receiver.
	CurTerm int
	// True if follower contained entry matching prevLogIndex
	// and prevLogTerm
	Success bool
	// Used when appending entries fails for leader to rewind appending entries.
	// Only used when candidate succeeds.
	RewindIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Candidate failed to claim as leader.
	if args.LeaderTerm < rf.curTerm {
		reply.Success = false
		reply.CurTerm = rf.curTerm
		reply.RewindIndex = -1
		return
	}
	// Heartbeat succeed. Won't send rpc to itself.
	//if rf.state != LEADER {
	rf.curTerm = args.LeaderTerm
	reply.CurTerm = rf.curTerm
	rf.state = FOLLOWER
	rf.lastAccess = time.Now()
	// Heart beat rpc
	//if len(args.Entries) != 0 {
	// PrevLogIndex doesn't exist or doesn't match term
	if len(rf.logs) <= args.PrevLogIndex || (rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm) {
		searchIndex := 0
		if len(rf.logs) <= args.PrevLogIndex {
			searchIndex = len(rf.logs) - 1
		} else {
			searchIndex = args.PrevLogIndex - 1
		}
		// Will stop at 0 because all log entries start with 0 by default.
		for ; searchIndex >= 0; searchIndex = searchIndex - 1 {
			if rf.logs[searchIndex].Term <= args.PrevLogTerm {
				break
			}
		}
		reply.RewindIndex = searchIndex
		reply.Success = false
		return
	}

	// Start replicating logs
	//if len(args.Entries) > 0 {
	//&& rf.state != LEADER {
	// New logs index
	argsIdx := 0
	// TODO: check if this is in order
	// check index corresponding to real index
	for _, entry := range args.Entries {
		if (entry.Index >= len(rf.logs)) || (rf.logs[entry.Index].Term != entry.Term) {
			break
		}
		argsIdx = argsIdx + 1
	}
	if argsIdx < len(args.Entries) {
		rf.logs = rf.logs[:args.Entries[argsIdx].Index]
		for ; argsIdx < len(args.Entries); argsIdx = argsIdx + 1 {
			rf.logs = append(rf.logs, args.Entries[argsIdx])
			DPrintf("After replication on me: %v, %v:", rf.me, rf.logs)
		}
		go func() { rf.persist() }()
	}

	//}
	//}

	// Applying to state machine
	if args.LeaderCommitIndex > rf.commitIndex {
		// && rf.state != LEADER {
		if args.LeaderCommitIndex < rf.logs[len(rf.logs)-1].Index {
			rf.commitIndex = args.LeaderCommitIndex
			DPrintf("Update commit index on follower:%v, %v", rf.me, rf.commitIndex)
		} else {
			DPrintf("Update commit index on follower:%v, %v", rf.me, rf.commitIndex)
			rf.commitIndex = rf.logs[len(rf.logs)-1].Index
		}
		if rf.commitIndex > rf.lastApplied {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i = i + 1 {
				// apply message
				applyMsg := ApplyMsg{}
				applyMsg.CommandValid = true
				applyMsg.CommandIndex = i
				applyMsg.Command = rf.logs[i].Command
				DPrintf("Apply message Follower: %v one me: %v", rf.logs[i], rf.me)
				(*rf.applyCh) <- applyMsg
			}
			rf.lastApplied = rf.commitIndex
		}

	}
	reply.Success = true
	reply.RewindIndex = -1
	return
}

func (rf *Raft) appendEntries() {
	//updateMu := sync.Mutex{}
	//updateCond := sync.NewCond(&updateMu)
	//cancel := false
	//updateCommit := 0
	// update commit
	go func() {
		for {
			time.Sleep(10 * time.Millisecond)
			//updateMu.Lock()
			//TODO: Need to unblock when it is not leader anymore
			//for (updateCommit == 0) && !cancel {
			//	updateCond.Wait()
			//}
			// DPrintf("I got signal me: %v state %v", rf.me, rf.state)
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}
			// 1, 2, 2
			matchIndex := make([]int, len(rf.matchIndex))
			copy(matchIndex, rf.matchIndex)
			sort.Ints(matchIndex)
			// (5 + 1)/2 - 1 = 2
			// 1, 1, 1, 51, 51
			medianOne := (len(matchIndex)+1)/2 - 1
			//DPrintf("me %v commit index: %v", rf.me, rf.commitIndex)
			//DPrintf("medianOne %v Index %v", medianOne, matchIndex[medianOne])
			if matchIndex[medianOne] > rf.commitIndex && rf.logs[matchIndex[medianOne]].Term == rf.curTerm {
				DPrintf("Leader commit Index me: %v, commitIndex before:%v, matchIndex:%v", rf.me, rf.commitIndex, rf.matchIndex)
				rf.commitIndex = matchIndex[medianOne]
				DPrintf("update commit index %v for %v", rf.commitIndex, rf.me)
			}
			if rf.commitIndex > rf.lastApplied {
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i = i + 1 {
					applyMsg := ApplyMsg{}
					applyMsg.CommandValid = true
					applyMsg.CommandIndex = i
					applyMsg.Command = rf.logs[i].Command
					DPrintf("Apply message Leader: %v on me %v", rf.logs[i], rf.me)
					(*rf.applyCh) <- applyMsg
				}
				rf.lastApplied = rf.commitIndex
			}
			rf.mu.Unlock()
			//updateCommit = 0
			//updateMu.Unlock()

		}
	}()
	go func() {
		for {
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				// updateMu.Lock()
				/// cancel = true
				//updateCond.Signal()
				//updateMu.Unlock()
				return
			}
			args := AppendEntriesArgs{}
			args.LeaderId = rf.me
			args.LeaderTerm = rf.curTerm
			args.LeaderCommitIndex = -1
			args.PrevLogIndex = 0
			args.PrevLogTerm = 0
			//args.LeaderCommitIndex = rf.commitIndex
			rf.mu.Unlock()
			// Heart beat message.
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					rf.mu.Lock()
					if rf.state == LEADER {
						rf.lastAccess = time.Now()
					}
					rf.mu.Unlock()
					continue
				}
				go func(i int, args AppendEntriesArgs) {
					rf.mu.Lock()
					if rf.state != LEADER {
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					reply := AppendEntriesReply{}
					ok := rf.callAppendEntries(i, &args, &reply)
					if ok && !reply.Success {
						rf.mu.Lock()
						if rf.state == LEADER {
							DPrintf("I am not a leader anymore: %v", rf.me)
							rf.state = FOLLOWER
							rf.curTerm = reply.CurTerm
						}
						rf.mu.Unlock()
					}
				}(i, args)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	for {
		rf.mu.Lock()
		if rf.state != LEADER {
			rf.mu.Unlock()
			return
		}
		args := AppendEntriesArgs{}
		args.LeaderId = rf.me
		args.LeaderTerm = rf.curTerm
		args.LeaderCommitIndex = rf.commitIndex
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			rf.mu.Lock()
			if i == rf.me {
				if rf.matchIndex[i] != (len(rf.logs) - 1) {
					rf.matchIndex[i] = (len(rf.logs) - 1)
					rf.nextIndex[i] = rf.matchIndex[i] + 1
				}
				rf.mu.Unlock()
				continue
			}
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			go func(i int, args AppendEntriesArgs) {
				rf.mu.Lock()
				if rf.nextIndex[i] < len(rf.logs) {
					args.Entries = rf.logs[rf.nextIndex[i]:]
				}
				args.PrevLogIndex = rf.nextIndex[i] - 1
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
				args.LeaderCommitIndex = rf.commitIndex
				rf.mu.Unlock()
				// not send rpc if cancel?
				reply := AppendEntriesReply{}
				ok := rf.callAppendEntries(i, &args, &reply)
				if ok {
					rf.mu.Lock()
					if rf.state != LEADER || args.PrevLogIndex != (rf.nextIndex[i]-1) || args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
						rf.mu.Unlock()
						return
					}
					if reply.CurTerm > rf.curTerm {
						DPrintf("I am not a leader anymore: %v", rf.me)
						rf.state = FOLLOWER
						rf.curTerm = reply.CurTerm
						rf.mu.Unlock()
						go func() { rf.persist() }()
						return
					}
					if len(args.Entries) > 0 && reply.Success {
						DPrintf("%v Updating matchIndex %v, my term %v, reply Term %v", rf.me, rf.matchIndex, rf.curTerm, reply.CurTerm)
						rf.nextIndex[i] = args.Entries[len(args.Entries)-1].Index + 1
						rf.matchIndex[i] = rf.nextIndex[i] - 1
						rf.mu.Unlock()
						return
					}
					if !reply.Success && reply.RewindIndex != -1 {
						DPrintf("RewindIndex reply: %v, %v", i, reply.RewindIndex)
						rf.nextIndex[i] = reply.RewindIndex + 1
					}
					rf.mu.Unlock()
					return
				}
			}(i, args)
		}
		time.Sleep(50 * time.Millisecond)
	}
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.pp
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

func (rf *Raft) callAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := 0
	term := 0
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := (rf.state == LEADER)
	if isLeader {
		term = rf.curTerm
		index = len(rf.logs)
		entry := logEntry{}
		entry.Index = index
		entry.Command = command
		entry.Term = term
		rf.logs = append(rf.logs, entry)
		go func() { rf.persist() }()
		rf.nextIndex[rf.me] = len(rf.logs)
		rf.matchIndex[rf.me] = len(rf.logs) - 1
		DPrintf("Send on index and term: %v, %v Leader me %v : logs %v, matchIndex: %v", index, term, rf.me, rf.logs, rf.matchIndex)
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
func (rf *Raft) checkElectionTimeout() {
	for {
		time.Sleep(100 * time.Millisecond)
		// check time now
		now := time.Now()
		rf.mu.Lock()
		if now.Sub(rf.lastAccess) > rf.duration {
			rf.state = CANDIDATE
			rf.mu.Unlock()
			r := rand.Intn(500) + 500
			rf.duration = time.Duration(r) * time.Millisecond
			return
		}
		rf.mu.Unlock()

	}
}

func (rf *Raft) startElection() {
	for {
		rf.mu.Lock()
		if rf.state != CANDIDATE {
			rf.mu.Unlock()
			return
		}
		rf.votedFor = rf.me
		rf.curTerm = rf.curTerm + 1
		args := RequestVoteArgs{}
		args.CandTerm = rf.curTerm
		args.CandidateId = rf.me
		args.LastLogIndex = len(rf.logs) - 1
		args.LastLogTerm = rf.logs[args.LastLogIndex].Term
		rf.mu.Unlock()
		go func() { rf.persist() }()
		voteMu := sync.Mutex{}
		cond := sync.NewCond(&voteMu)
		voteYes := 0
		voteDone := 0
		done := make(chan string)
		cancel := false
		for i := 0; i < len(rf.peers); i++ {
			go func(i int) {
				reply := RequestVoteReply{}
				succeed := rf.sendRequestVote(i, &args, &reply)
				voteMu.Lock()
				defer voteMu.Unlock()
				voteDone = voteDone + 1
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if succeed && rf.state == CANDIDATE {
					if reply.CurTerm > rf.curTerm {
						rf.state = FOLLOWER
						rf.curTerm = reply.CurTerm
						done <- "cancel"
						cancel = true
						go func() { rf.persist() }()
					}
					if reply.VoteGranted {
						voteYes = voteYes + 1
					}

				}
				cond.Signal()
			}(i)
		}

		go func() {
			voteMu.Lock()
			defer voteMu.Unlock()
			// count votes
			for voteYes <= (len(rf.peers)/2) && voteDone != len(rf.peers) && !cancel {
				cond.Wait()
			}
			if !cancel {
				done <- "done"
			}
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}

		rf.mu.Lock()
		if (args.CandTerm == rf.curTerm) && (rf.state == CANDIDATE) && !cancel && (voteYes > (len(rf.peers) / 2)) {
			rf.state = LEADER
		}
		rf.mu.Unlock()

	}

}

func (rf *Raft) Init() {
	for {
		switch rf.state {
		case FOLLOWER:
			{
				rf.checkElectionTimeout()
			}
		case CANDIDATE:
			{
				rf.startElection()
			}
		case LEADER:
			{
				rf.appendEntries()
			}
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.applyCh = &applyCh
	rf.curTerm = 0
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)
	r := rand.Intn(500) + 500
	rf.duration = time.Duration(r) * time.Millisecond
	rf.lastAccess = time.Now()
	for _, _ = range peers {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	log := logEntry{}
	log.Index = 0
	log.Term = 0
	rf.logs = append(rf.logs, log)

	rf.commitIndex = 0
	rf.lastApplied = 0

	// Your initialization code here (2A, 2B, 2C).
	go rf.Init()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
