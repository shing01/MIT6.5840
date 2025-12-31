package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	// "fmt"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

const (
	StateFollower  = 0
	StateCandidate = 1
	StateLeader	   = 2
)

const (
	ElectionTimeoutMin = 150 * time.Millisecond
	ElectionTimeoutMax = 300 * time.Millisecond
)

type LogEntry struct {
	Term 	int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm  int
	voteFor	 	 int
	log			 []LogEntry

	// Volatile state on all servers
	state		 int
	lastElection time.Time
	commitIndex  int
	lastApplied  int

	// Volatile state on leaders
	nextIndex   []int
	matchIndex  []int

	applyCh chan raftapi.ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.state == StateLeader {
		isleader = true
	}

	return term, isleader
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term 	     int // candidate's term
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term 		int  // current term, for candidate to update itself
	VoteGranted bool // if get vote or not
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = StateFollower
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}

	reply.Term = rf.currentTerm

	// 获取本地最后一条日志的 Index 和 Term
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	// 判断 Candidate 是否足够新
	isUpToDate := false
	if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
		isUpToDate = true
	}

	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && isUpToDate {
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		rf.lastElection = time.Now()
	} else {
		reply.VoteGranted = false
	}
}

type AppendEntriesArgs struct {
	Term	     int 		// leader's term
	LeaderId     int 		// so follower can redirect clients
	PrevLogIndex int 		// index of log entry immediately preceding new ones
	PrevLogTerm  int 		// term of prevLogIndex entry
	Entries		 []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int 		// leader’s commitIndex
}

type AppendEntriesReply struct {
	Term	int // current term, for leader to update itself
	Success bool
}

// example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.voteFor = -1
		rf.currentTerm = args.Term
	}

	rf.state = StateFollower
	rf.lastElection = time.Now()

	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		if index < len(rf.log) {
			if rf.log[index].Term != entry.Term {
				rf.log = rf.log[:index]
				rf.log = append(rf.log, entry)
			}
		} else {
			rf.log = append(rf.log, entry)
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		newCommitIndex := min(args.LeaderCommit, args.PrevLogIndex + len(args.Entries))
		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = newCommitIndex
		}
	}

	reply.Success = true
	reply.Term = rf.currentTerm
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if rf.state != StateLeader {
		isLeader = false
		return index, term, isLeader
	}

	entry := LogEntry{
		Term: 	 rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	index = len(rf.log) - 1
	term = rf.currentTerm
	// fmt.Printf("S%d Start command index %d term %d\n", rf.me, index, term)

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
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

func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.state = StateCandidate
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.lastElection = time.Now()

	currentTerm := rf.currentTerm // 保存当前任期，用于后续RPC请求的过期检查
	votes := 1
	totalPeers := len(rf.peers)
	majority := totalPeers/2 + 1

	rf.mu.Unlock()

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()

			lastLogIndex := len(rf.log) - 1
			lastLogTerm := rf.log[lastLogIndex].Term
			args := RequestVoteArgs{
				Term: 		  rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			rf.mu.Unlock()

			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(server, &args, &reply); !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 过期检查: 状态已改变或任期已更新
			if rf.state != StateCandidate || rf.currentTerm != currentTerm {
				return
			}

			//Term检查: 发现更高任期
			if reply.Term > rf.currentTerm {
				rf.state = StateFollower
				rf.currentTerm = reply.Term
				rf.voteFor = -1
				return
			}

			if reply.VoteGranted {
				votes++
				if votes >= majority && rf.state == StateCandidate {
					rf.state = StateLeader
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					lastLogIndex := len(rf.log)
					for i := range rf.peers {
						rf.nextIndex[i] = lastLogIndex
						rf.matchIndex[i] = 0
					}

					// fmt.Printf("S%d Become Leader Term %d\n", rf.me, rf.currentTerm)
					go rf.sendHeartbeats()
				}
			}
		}(server)
	}
	time.Sleep(50 * time.Millisecond)
}

func (rf *Raft) sendHeartbeats() {
	for rf.killed() == false {
		rf.mu.Lock()

		if rf.state != StateLeader {
			rf.mu.Unlock()
			return
		}

		currentTerm := rf.currentTerm
		rf.mu.Unlock()

		for server := range rf.peers {
			if server == rf.me {
				continue
			}

			go func(server int) {
				rf.mu.Lock()

				if rf.nextIndex[server] > len(rf.log) {
					rf.nextIndex[server] = len(rf.log)
				}

				prevLogIndex := rf.nextIndex[server] - 1
				prevLogTerm := rf.log[prevLogIndex].Term
				args := AppendEntriesArgs{
					Term: 	  	  currentTerm,
					LeaderId: 	  rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:	  rf.log[rf.nextIndex[server]:],
					LeaderCommit: rf.commitIndex,
				}

				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, &args, &reply)

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != StateLeader || rf.currentTerm != currentTerm {
					return
				}

				if ok && rf.currentTerm < reply.Term {
					rf.state = StateFollower
					rf.currentTerm = reply.Term
					rf.voteFor = -1
					return
				}

				if reply.Success {
					newNextIndex := args.PrevLogIndex + len(args.Entries) + 1
					newMatchIndex := args.PrevLogIndex + len(args.Entries)
					if rf.nextIndex[server] < newNextIndex {
						rf.nextIndex[server] = newNextIndex
					}
					if rf.matchIndex[server] < newMatchIndex {
						rf.matchIndex[server] = newMatchIndex
					}

					for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
						count := 1;
						for i := range rf.peers {
							if i != rf.me && rf.matchIndex[i] >= N {
								count++
							}
						}
						if count > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
							rf.commitIndex = N
							break
						}
					}
				} else {
					if rf.nextIndex[server] > 1 {
						rf.nextIndex[server]--
					}
					// rf.matchIndex[server] = rf.nextIndex[server] - 1 
				}
			}(server)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state != StateLeader && time.Since(rf.lastElection) > ElectionTimeoutMax {
			rf.mu.Unlock()
			rf.startElection()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()

		// 等有新条目需要提交
		for rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()
		}

		rf.lastApplied++
		if rf.lastApplied < len(rf.log) {
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:	  rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}

			rf.mu.Unlock()
			rf.applyCh <- msg
		} else {
			rf.mu.Unlock()
		}
	}
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = StateFollower
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.lastElection = time.Now()

	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0}
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh 

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
