package raft

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"io/ioutil"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
	"os"
)

var debug = true

//Lock for vote counting
var voteLock = &sync.RWMutex{}

var voteMap = make(map[int]bool)

type Lsn uint64 //Log sequence number, unique for all time.

//LeaderID denotes id of the leader
var LeaderID, Quorum int

//Below constants are used to set message IDs.
const (
	APPENDENTRIESRPC      = 1
	CONFIRMCONSENSUS      = 2
	REQUESTVOTE           = 3
	VOTERESPONSE          = 4
	APPENDENTRIES         = 5
	APPENDENTRIESRESPONSE = 6
	HEARTBEATRESPONSE     = 7
	HEARTBEAT             = 8
	APPENDENTRIESRPCRESPONSE      = 9
)

const (
	NOVOTE         = -1
	UNKNOWN        = -1
	INITIALIZATION = 0
)

//Below constants define state of the raft server
const (
	FOLLOWER  = 1
	CANDIDATE = 2
	LEADER    = 3
)

const (
	BROADCAST = -1
)

var (
	/*
	   Both candidate and follower state raft server will wait for random time between min and max election timeout
	   and once it's wait is over, will start election all over again.
	*/
	MinElectTo int32 = 4000
	MaxElectTo       = 3 * MinElectTo
)

type ServerConfig struct {
	Id         string
	HostName   string
	ClientPort string
	LogPort    string
}

type clusterCount struct {
	Count string
}

type serverLogPath struct {
	Path string
}

type ClusterConfig struct {
	Path    serverLogPath  // Directory for persistent log
	Servers []ServerConfig // All servers in this cluster
	Count   clusterCount
}

//Raft is a structure that defines server and also maintains information about cluster.
type Raft struct {
	Pid           int
	Peers         []int
	Path          string
	Term          uint64 // Current term ID for raft object.(Monotonic increase)
	VotedFor      int // Server ID for which this raft has voted in leader election.
	VotedTerm     int
	LeaderId      int            //Current Leader ID
	CommitIndex   uint64         //Index of the highest entry commited till time.(Monotonic increase)
	MatchIndex    map[int]uint64 //Index fo the highest log entry known to be replicated on server for all peers.
	NextIndex     map[int]uint64 //Index of the next log entry to send to that server for all peers (initialized to leader last log index + 1)
	PrevLogIndex  uint64
	PrevLogTerm   uint64
	ElectTicker   <-chan time.Time
	State         int
	LastApplied   uint64 //Index of the highest log entry applied to state machine.
	In            chan *Envelope
	Out           chan *Envelope
	Address       map[int]string
	ClientSockets map[int]*zmq.Socket
	LogSockets    map[int]*zmq.Socket
	LsnVar        uint64
	ClusterSize   int
	GotConsensus  chan bool
	SLog          *Log
	Inchan chan *[]byte
        Outchan chan interface{}
}

//store index of all peer servers
type nextIndex struct {
	sync.RWMutex
	m map[uint64]uint64 // followerId: nextIndex
}

//type appendEntriesResponse struct{}

//Server interface declares functions that will be used to provide APIs to comminicate with server.
type Server interface {
	ServId() int

	PeersIds() []int

	GetTerm() uint64 //Returns current term for the shared log for this raft server.

	GetLeader() int //Returns leader id for the current term.

	GetVotedFor() int //Returns ID of candidate for which raft server has voted for.

	GetCommitIndex() uint64

	GetLastApplied() uint64

	Start()

	Outbox() chan *Envelope

	Inbox() chan *Envelope

	No_Of_Peers() int

	ClientServerComm(clientPortAddr string)

	RetLsn() Lsn

	GetState() int

	resetElectTimeout() // resets election timeout

	loop()

	follower() //Go routine which is run when server enters into follower state at boot up and also later in its life

	candidate()

	leader()

	GetPrevLogIndex() uint64

	GetPrevLogTerm() uint64

	SetTerm(uint64)

	SetVotedFor(int)

	requestForVoteToPeers()

	handleRequestVote(env *Envelope) bool

	sendHeartBeats(ni *nextIndex) (int, bool)

	handleAppendEntries(env *Envelope) (*appendEntriesResponse, bool)
}

func (ServerVar *Raft) Start() {

	ServerVar.loop()

}

func (ServerVar *Raft) SetTerm(term uint64) {
	ServerVar.Term = term

}

func (ServerVar *Raft) GetPrevLogIndex() uint64 {
	return (*ServerVar).PrevLogIndex
}

func (ServerVar *Raft) GetPrevLogTerm() uint64 {
	return (*ServerVar).PrevLogTerm
}

func (ServerVar *Raft) GetLastApplied() uint64 {
	return (*ServerVar).LastApplied
}

func (ServerVar *Raft) GetState() int {
	return (*ServerVar).State
}

func (ServerVar *Raft) GetCommitIndex() uint64 {
	return (*ServerVar).CommitIndex
}

func (ServerVar *Raft) GetTerm() uint64 {
	return (*ServerVar).Term
}

func (ServerVar *Raft) GetLeader() int {
	return (*ServerVar).LeaderId
}

func (ServerVar *Raft) GetVotedFor() int {
	return (*ServerVar).VotedFor
}

func (ServerVar *Raft) SetVotedFor(vote int) {

	(*ServerVar).VotedFor = vote
}

func (ServerVar *Raft) RetLsn() Lsn {

	return Lsn((*ServerVar).LsnVar)
}

func (ServerVar *Raft) No_Of_Peers() int {
	return (*ServerVar).ClusterSize
}

func (ServerVar *Raft) Outbox() chan *Envelope {
	return (*ServerVar).Out
}

func (ServerVar *Raft) Inbox() chan *Envelope {
	return (*ServerVar).In
}

func (ServerVar *Raft) ServId() int {
	return (*ServerVar).Pid
}

func (ServerVar *Raft) PeersIds() []int {
	return (*ServerVar).Peers
}

func (ServerVar *Raft) loop() {

	for {

		state := ServerVar.GetState() // begin life as a follower

		switch state {
		case FOLLOWER:
			ServerVar.follower()

		case CANDIDATE:
			ServerVar.candidate()

		case LEADER:
			ServerVar.leader()

		default:
			return
		}
	}
}

func (ServerVar *Raft) catchUpLog(ni *nextIndex, id int) error {

	
	//curLsn := ServerVar.SLog.getCurrentIndex() + 1
	//resultentries, lastTerm := ServerVar.SLog.entriesAfter(curLsn)
	currentTerm := ServerVar.Term

	prevLogIndex := ni.prevLogIndex(uint64(id))

	resultentries, _ := ServerVar.SLog.entriesAfter(prevLogIndex,10)

	//commitIndex := ServerVar.SLog.getCommitIndex()

	var envVar Envelope
	envVar.Pid = id
	envVar.MessageId = APPENDENTRIES
	envVar.SenderId = ServerVar.ServId()
	envVar.Leaderid = ServerVar.LeaderId
	envVar.LastLogIndex = ServerVar.GetPrevLogIndex()
	envVar.LastLogTerm = ServerVar.GetPrevLogTerm()
	envVar.CommitIndex = ServerVar.GetCommitIndex()
	envVar.Message = &appendEntries{TermIndex: currentTerm, Entries: resultentries}
	ServerVar.Outbox() <- &envVar

	select {

	//case <-replication:

	case env := <-(ServerVar.Inbox()):
		switch env.MessageId {

		case APPENDENTRIESRESPONSE:

			/*if debug {

				fmt.Println("Received heartbeat response from %v", id)
			}
*/
			resp := env.Message.(appendEntriesResponse)
			if resp.Term > currentTerm {
				return errorDeposed
			}

			if !resp.Success {
				newPrevLogIndex, err := ni.decrement(uint64(id), prevLogIndex)
				fmt.Println("new index for ", id, "is", newPrevLogIndex)
				if err != nil {

					return err
				}
				if debug {
					fmt.Println("flush to %v: rejected")
				}
				return errorappendEntriesRejected
			}

			if len(resultentries) > 0 {
				newPrevLogIndex, err := ni.set(uint64(id), uint64(resultentries[len(resultentries)-1].Lsn()), prevLogIndex)
				fmt.Println("new index for ", id, "is", newPrevLogIndex)
				if err != nil {

					return err
				}

				return nil
			}
			return nil

		}

	}
	return nil
}

func (ServerVar *Raft) handleAppendEntries(env *Envelope) (*appendEntriesResponse, bool) {

	resp := env.Message.(appendEntries)
	if debug {
		fmt.Println("handleAppendEntries() : Fo server ", ServerVar.ServId(), " Term : ", ServerVar.Term, " Requested Term = ", resp.TermIndex, "Coommit index ", env.CommitIndex, " ServerVar.SLog.commitIndex ", ServerVar.SLog.commitIndex)
	}

	if resp.TermIndex < ServerVar.Term {

	
		return &appendEntriesResponse{
			Term:    ServerVar.Term,
			Success: false,
			reason:  fmt.Sprintf("Term is less"),
		}, false
	}

	//success := true
	downGrade := false

	if resp.TermIndex > ServerVar.Term {
		ServerVar.Term = resp.TermIndex
		ServerVar.VotedFor = NOVOTE
		downGrade = true

	}

	if ServerVar.State == CANDIDATE && env.SenderId != ServerVar.LeaderId && resp.TermIndex >= ServerVar.Term {
		ServerVar.Term = resp.TermIndex
		ServerVar.VotedFor = NOVOTE
		downGrade = true
	}
	ServerVar.resetElectTimeout()

	
	if err := ServerVar.SLog.discardEntries(env.LastLogIndex, env.LastLogTerm); err != nil {
	
		return &appendEntriesResponse{
			Term:    ServerVar.Term,
			Success: false,
			reason:  fmt.Sprintf("while ensuring last log entry had index=%d term=%d: error: %s", env.LastLogIndex, env.LastLogTerm, err)}, downGrade
	}

	
	//resp := env.Message.(appendEntries)

	for i, entry := range resp.Entries {
fmt.Println("Number of times ",i)
		if err := ServerVar.SLog.appendEntry(entry); err != nil {

			
			return &appendEntriesResponse{
				Term:    ServerVar.Term,
				Success: false,
				reason: fmt.Sprintf(
					"AppendEntry %d/%d failed: %s",
					i+1,
					len(resp.Entries),
					err,
				),
			}, downGrade

		}

	}

	if env.CommitIndex > 0 && env.CommitIndex > ServerVar.SLog.commitIndex {

		if err := ServerVar.SLog.commitTill(env.CommitIndex); err != nil {

			return &appendEntriesResponse{
				Term:    ServerVar.Term,
				Success: false,
				reason:  fmt.Sprintf("CommitTo failed"),
			}, downGrade
		}

	}


	return &appendEntriesResponse{
		Term:    ServerVar.Term,
		Success: true,
	}, downGrade

}

var (
	errorTimeout               = errors.New("Time out while log replication")
	errorDeposed               = errors.New("Deposed during replication")
	errorOutOfSync             = errors.New("Out of sync")
	errorappendEntriesRejected = errors.New("AppendEntries RPC rejected")
)

type appendEntries struct {
	TermIndex uint64
	Entries   []*LogEntryStruct
}

// appendEntriesResponse represents the response to an appendEntries RPC.
type appendEntriesResponse struct {
	Term    uint64
	Success bool
	reason  string
}

func (ServerVar *Raft) newNextIndex(defaultNextIndex uint64) *nextIndex {
	ni := &nextIndex{
		m: map[uint64]uint64{},
	}
	for _, id := range ServerVar.PeersIds() {
		ni.m[uint64(id)] = defaultNextIndex
	}
	return ni
}

func (ni *nextIndex) prevLogIndex(id uint64) uint64 {
	ni.RLock()
	defer ni.RUnlock()
	if _, ok := ni.m[id]; !ok {
		panic(fmt.Sprintf("peer %d not found", id))
	}
	return ni.m[id]
}

func (ni *nextIndex) decrement(id uint64, prev uint64) (uint64, error) {
	ni.Lock()
	defer ni.Unlock()
	i, ok := ni.m[id]
	if !ok {
		panic(fmt.Sprintf("peer %d not found", id))
	}
	if i != prev {
		return i, errorOutOfSync
	}
	if i > 0 {
		ni.m[id]--
	}
	return ni.m[id], nil
}

func (ni *nextIndex) set(id, index, prev uint64) (uint64, error) {
	ni.Lock()
	defer ni.Unlock()
	i, ok := ni.m[id]
	if !ok {
		panic(fmt.Sprintf("server %d not found", id))
	}
	if i != prev {
		return i, errorOutOfSync
	}
	ni.m[id] = index
	return index, nil
}

func (ServerVar *Raft) sendHeartBeats(ni *nextIndex) (int, bool) {

	type tuple struct {
		id  uint64
		err error
	}
	responses := make(chan tuple, len(ServerVar.PeersIds()))
	for _, id1 := range ServerVar.PeersIds() {
		go func(id int) {
			errChan := make(chan error, 1)
			go func() { errChan <- ServerVar.catchUpLog(ni, id) }()
			responses <- tuple{uint64(id), <-errChan}
		}(id1)
	}
	successes, downGrade := 0, false
	for i := 0; i < cap(responses); i++ {
		switch t := <-responses; t.err {
		case nil:
			successes++
		case errorDeposed:
			downGrade = true
		default:
		}
	}
	return successes, downGrade

}

func (ServerVar *Raft) leader() {

	//replicate := make(chan struct{})

	replicate := make(chan struct{})

	hbeat := time.NewTicker(heartBeatInterval())

	defer hbeat.Stop()
	go func() {
		for _ = range hbeat.C {
			replicate <- struct{}{}
		}
	}()

	nIndex := ServerVar.newNextIndex(uint64(ServerVar.SLog.lastIndex()))

	//go ServerVar.sendHeartBeats(nIndex)

	for {
		select {
		
		
		case t := <-ServerVar.Outchan:
cmd := t.(Command)
// Append the command to our (leader) log
fmt.Println("got command, appending", ServerVar.Term)
currentTerm := ServerVar.Term

		comma := new(bytes.Buffer)
		encCommand := gob.NewEncoder(comma)
		encCommand.Encode(cmd)

entry := &LogEntryStruct{
		Logsn: Lsn(ServerVar.SLog.lastIndex() + 1),
		TermIndex: currentTerm,
		DataArray: comma.Bytes(),
		//Commit : <- false,//cmd.CommandResponse,
		}

		if err := ServerVar.SLog.appendEntry(entry); err != nil {
		panic(err)
		continue
		}

fmt.Printf(
"after append, commitIndex=%d lastIndex=%d lastTerm=%d",
ServerVar.SLog.getCommitIndex(),
ServerVar.SLog.lastIndex(),
ServerVar.SLog.lastTerm(),
)
go func() { replicate <- struct{}{} }()
		
		

		case <-replicate:

			successes, downGrade := ServerVar.sendHeartBeats(nIndex)

			if downGrade {

				//As leader downgrade will result in having unknown leader and going into follower state
				ServerVar.LeaderId = UNKNOWN
				ServerVar.State = FOLLOWER

				return
			}

			if successes >= Quorum-1 {

			}

		case env := <-(ServerVar.Inbox()):
			switch env.MessageId {

			case REQUESTVOTE:

				if debug {
					fmt.Println("Received request vote for candidate....")
				}

				downgrade := ServerVar.handleRequestVote(env)

				if downgrade {

					//As leader downgrade will result in having unknown leader and going into follower state
					ServerVar.LeaderId = UNKNOWN
					ServerVar.State = FOLLOWER

					return
				}

			case VOTERESPONSE:

			case APPENDENTRIES:

				resp, down := ServerVar.handleAppendEntries(env)

				var envVar Envelope
				envVar.Pid = env.Leaderid
				envVar.MessageId = APPENDENTRIESRESPONSE
				envVar.SenderId = ServerVar.ServId()
				envVar.Leaderid = ServerVar.LeaderId
				envVar.LastLogIndex = ServerVar.GetPrevLogIndex()
				envVar.LastLogTerm = ServerVar.GetPrevLogTerm()
				envVar.CommitIndex = ServerVar.GetCommitIndex()
				envVar.Message = resp
				
				//fmt.Println("Before sending  ",ServerVar.ServId())
				ServerVar.Outbox() <- &envVar
				//fmt.Println("After sending  ",ServerVar.ServId())
				//TODO: handle and count sucesses

				if down {
					ServerVar.LeaderId = env.Leaderid
					ServerVar.State = FOLLOWER
					return

				}

			case APPENDENTRIESRPC:
				resp, down := ServerVar.handleAppendEntries(env)

				var envVar Envelope
				envVar.Pid = env.Leaderid
				envVar.MessageId = APPENDENTRIESRPCRESPONSE
				envVar.SenderId = ServerVar.ServId()
				envVar.Leaderid = ServerVar.LeaderId
				envVar.LastLogIndex = ServerVar.GetPrevLogIndex()
				envVar.LastLogTerm = ServerVar.GetPrevLogTerm()
				envVar.CommitIndex = ServerVar.GetCommitIndex()
				envVar.Message = resp
				ServerVar.Outbox() <- &envVar

				//TODO: handle and count sucesses

				if down {
					ServerVar.LeaderId = env.Leaderid
					ServerVar.State = FOLLOWER
					return

				}
				
			case APPENDENTRIESRPCRESPONSE:
			fmt.Println("Received....Response.......................")
				resp := env.Message.(appendEntriesResponse)
				
				 ConfirmConsensus(ServerVar.ServId(), ServerVar,&resp)	
				

			}
		}

	}

}

func heartBeatInterval() time.Duration {
	tm := MinElectTo / 5
	return time.Duration(tm) * time.Millisecond
}

func (ServerVar *Raft) candidate() {

	ServerVar.requestForVoteToPeers()

	if debug {
		fmt.Println("CANDIDATE ID = ", ServerVar.ServId())
	}

	for {
		select {
		case <-ServerVar.ElectTicker:
			ServerVar.resetElectTimeout()
			ServerVar.Term++
			ServerVar.VotedFor = NOVOTE

			if debug {
				fmt.Println("TIMEOUT for CANDIDATE ID = ", ServerVar.ServId(), "New Term = ", ServerVar.Term)
			}

			return

		case env := <-(ServerVar.Inbox()):

			//les := env.Message.(LogEntryStruct)

			if debug {
				fmt.Println("CANDIDATE : Received Message is %v for %d ", env.MessageId, ServerVar.ServId())
			}

			switch env.MessageId {

			case REQUESTVOTE:

				if debug {
					fmt.Println("Received request vote for candidate....")
				}

				downgrade := ServerVar.handleRequestVote(env)

				if downgrade {

					//As a candidate downgrade will result in having unknown leader and going into follower state
					ServerVar.LeaderId = UNKNOWN
					ServerVar.State = FOLLOWER
					return
				}

			case VOTERESPONSE:
				les := env.Message.(LogEntryStruct)
				if les.TermIndex > ServerVar.Term {
					ServerVar.LeaderId = UNKNOWN
					ServerVar.State = FOLLOWER
					ServerVar.VotedFor = NOVOTE
					if debug {
						fmt.Println("Message term is greater for candidate = ", ServerVar.ServId(), " becoming follower")
					}

					return
				}

				if les.TermIndex < ServerVar.Term {
					break
				}

				voteLock.Lock()
				voteMap[env.SenderId] = true
				voteLock.Unlock()

				vcount := 0
				for i := range ServerVar.PeersIds() {
					if voteMap[ServerVar.Peers[i]] == true {
						vcount++
					}

				}
				if debug {
					fmt.Println(" Candidate Server id = ", ServerVar.ServId(), " vcount = ", vcount, " Quorum = ", Quorum)

				}

				if vcount >= Quorum-1 {
					ServerVar.LeaderId = ServerVar.ServId()
					ServerVar.State = LEADER
					ServerVar.VotedFor = NOVOTE
					if debug {
						fmt.Println(" New Leader Server id = ", ServerVar.ServId())

					}

					return
				}

			case APPENDENTRIES:

				resp, down := ServerVar.handleAppendEntries(env)

				var envVar Envelope
				envVar.Pid = env.Leaderid
				envVar.MessageId = APPENDENTRIESRESPONSE
				envVar.SenderId = ServerVar.ServId()
				envVar.Leaderid = ServerVar.LeaderId
				envVar.LastLogIndex = ServerVar.GetPrevLogIndex()
				envVar.LastLogTerm = ServerVar.GetPrevLogTerm()
				envVar.CommitIndex = ServerVar.GetCommitIndex()
				envVar.Message = resp
				ServerVar.Outbox() <- &envVar

				if down {
					ServerVar.LeaderId = env.Leaderid
					ServerVar.State = FOLLOWER
					return

				}

			case APPENDENTRIESRPC:

				if debug {
					fmt.Println("CANDIDATE : Processing Message is %v for %d ", env.MessageId, ServerVar.ServId())
				}

				resp, down := ServerVar.handleAppendEntries(env)

				var envVar Envelope
				envVar.Pid = env.Leaderid
				envVar.MessageId = APPENDENTRIESRPCRESPONSE
				envVar.SenderId = ServerVar.ServId()
				envVar.Leaderid = ServerVar.LeaderId
				envVar.LastLogIndex = ServerVar.GetPrevLogIndex()
				envVar.LastLogTerm = ServerVar.GetPrevLogTerm()
				envVar.CommitIndex = ServerVar.GetCommitIndex()
				envVar.Message = resp
				
				ServerVar.Outbox() <- &envVar

				if debug {
					fmt.Println("CANDIDATE : Sending Message is %v for %d ", env.MessageId, ServerVar.ServId())
				}
				//TODO: handle and count sucesses

				if down {
					ServerVar.LeaderId = env.Leaderid
					ServerVar.State = FOLLOWER
					return

				}

			}

		}

	}
}

func (ServerVar *Raft) requestForVoteToPeers() {
	
	
	var lesn LogEntryStruct

	(*ServerVar).LsnVar = (*ServerVar).LsnVar + 1
	lesn.Logsn = Lsn((*ServerVar).LsnVar)
	lesn.DataArray = nil
	lesn.TermIndex = ServerVar.GetTerm()
	
        lesn.Commit = nil
	
		var envVar Envelope
	envVar.Pid = BROADCAST
	envVar.MessageId = REQUESTVOTE
	envVar.SenderId = ServerVar.ServId()
	envVar.LastLogIndex = ServerVar.GetPrevLogIndex()
	envVar.LastLogTerm = ServerVar.GetPrevLogTerm()
	envVar.Message = lesn
	//TODO: Whats below line??
	//MsgAckMap[les.Lsn()] = 1
	ServerVar.VotedFor = ServerVar.ServId()
	
	
	ServerVar.Outbox() <- &envVar
	
	
}

func (ServerVar *Raft) follower() {

	if debug {
		fmt.Println(" Follower ID = ", ServerVar.ServId())
	}

	ServerVar.resetElectTimeout()

	for {

		select {

		case <-ServerVar.ElectTicker:
			ServerVar.Term++
			ServerVar.VotedFor = NOVOTE
			ServerVar.LeaderId = UNKNOWN
			ServerVar.resetElectTimeout()
			ServerVar.State = CANDIDATE

			if debug {
				fmt.Println("TIMEOUT for Follower ID = ", ServerVar.ServId(), " Now Candidate")
			}
			return

		case env := <-(ServerVar.Inbox()):

			

			//les := LogEntryStruct(env.Message)

			switch env.MessageId {

			case REQUESTVOTE:

				downgrade := ServerVar.handleRequestVote(env)

				if downgrade {

					//As a follower downgrade will result in having unknown leader
					ServerVar.LeaderId = UNKNOWN
				}

			case VOTERESPONSE:
			//TODO:
			case APPENDENTRIES:

				if ServerVar.LeaderId == UNKNOWN {
					ServerVar.LeaderId = env.SenderId

				}

				resp, down := ServerVar.handleAppendEntries(env)

				var envVar Envelope
				envVar.Pid = env.Leaderid
				envVar.MessageId = APPENDENTRIESRESPONSE
				envVar.SenderId = ServerVar.ServId()
				envVar.Leaderid = ServerVar.LeaderId
				envVar.LastLogIndex = ServerVar.GetPrevLogIndex()
				envVar.LastLogTerm = ServerVar.GetPrevLogTerm()
				envVar.CommitIndex = ServerVar.GetCommitIndex()
				envVar.Message = resp
				
				fmt.Println("B4 Sending ",ServerVar.ServId())
				ServerVar.Outbox() <- &envVar
fmt.Println("After Sending ",ServerVar.ServId())
				//TODO: handle and count sucesses

				if down {
					ServerVar.LeaderId = env.Leaderid
					ServerVar.State = FOLLOWER
					return

				}

			case APPENDENTRIESRPC:			

				resp, down := ServerVar.handleAppendEntries(env)
				var envVar Envelope
				envVar.Pid = env.Leaderid
				envVar.MessageId = APPENDENTRIESRPCRESPONSE
				envVar.SenderId = ServerVar.ServId()
				envVar.Leaderid = ServerVar.LeaderId
				envVar.LastLogIndex = ServerVar.GetPrevLogIndex()
				envVar.LastLogTerm = ServerVar.GetPrevLogTerm()
				envVar.CommitIndex = ServerVar.GetCommitIndex()
				envVar.Message = resp
				ServerVar.Outbox() <- &envVar

				//TODO: handle and count sucesses

				if down {
					ServerVar.LeaderId = env.Leaderid
					ServerVar.State = FOLLOWER
					return

				}

			}

		}
	}

}

func (serverVar *Raft) handleRequestVote(req *Envelope) bool {

	resp := req.Message.(LogEntryStruct)

	if resp.TermIndex < serverVar.Term {
		return false
	}

	downgrade := false

	if resp.TermIndex > serverVar.Term {
		if debug {
			fmt.Println("RequestVote from newer term (%d): my term %d", resp.TermIndex, serverVar.Term)
		}
		serverVar.Term = resp.TermIndex
		serverVar.VotedFor = NOVOTE
		serverVar.LeaderId = UNKNOWN
		downgrade = true
	}

	if serverVar.GetState() == LEADER && !downgrade {

		return false
	}

	if serverVar.VotedFor != NOVOTE && serverVar.VotedFor != req.SenderId {

		return downgrade
	}

	if serverVar.PrevLogIndex > req.LastLogIndex || serverVar.PrevLogTerm > req.LastLogTerm {
		return downgrade
	}

	var lesn LogEntryStruct

	(*serverVar).LsnVar = (*serverVar).LsnVar + 1
	lesn.Logsn = Lsn((*serverVar).LsnVar)
	lesn.DataArray = nil
	lesn.TermIndex = serverVar.GetTerm()
	lesn.Commit = nil

	var envVar Envelope
	envVar.Pid = req.SenderId
	envVar.MessageId = VOTERESPONSE
	envVar.SenderId = serverVar.ServId()
	envVar.LastLogIndex = serverVar.GetPrevLogIndex()
	envVar.LastLogTerm = serverVar.GetPrevLogTerm()
	envVar.Message = lesn
	//TODO: Whats below line??
	//MsgAckMap[les.Lsn()] = 1

	if debug {

		fmt.Println("Sending vote for Candidate=", req.SenderId, " Term = ", lesn.TermIndex, " follower = ", envVar.SenderId)
	}

	serverVar.VotedFor = req.SenderId
	serverVar.resetElectTimeout()
	serverVar.Outbox() <- &envVar

	return downgrade
}

//FireAServer() starts a server with forking methods to listen at a port for intra cluster comminication and for client-server communication

func FireAServer(myid int) Server {

	fileName := "clusterConfig.json"
	var obj ClusterConfig
	file, e := ioutil.ReadFile(fileName)

	if e != nil {
		panic("File error: " + e.Error())
	}

	json.Unmarshal(file, &obj)



	logfile := os.Getenv("GOPATH") + "/log/log_" + strconv.Itoa(myid)

	tLog := createNewLog(logfile)

	serverVar := &Raft{
		Pid:           UNKNOWN,
		Peers:         make([]int, len(obj.Servers)-1),
		Path:          "",
		Term:          INITIALIZATION,
		VotedFor:      NOVOTE,
		VotedTerm:     UNKNOWN,
		LeaderId:      UNKNOWN,
		CommitIndex:   0,
		ElectTicker:   nil,
		PrevLogIndex:  0,
		PrevLogTerm:   0,
		MatchIndex:    make(map[int]uint64),
		NextIndex:     make(map[int]uint64),
		LastApplied:   0,
		State:         FOLLOWER, //Initialized server as FOLLOWER
		In:            make(chan *Envelope),
		Out:           make(chan *Envelope),
		Address:       map[int]string{},
		ClientSockets: make(map[int]*zmq.Socket),
		LsnVar:        0,
		LogSockets:    make(map[int]*zmq.Socket),
		Inchan: make(chan *[]byte, 1024),
		Outchan: make(chan interface{}),
		ClusterSize:   len(obj.Servers) - 1,
		GotConsensus:  make(chan bool),
		SLog:          tLog,
	}

	count := 0
	LeaderID = UNKNOWN
	Quorum = UNKNOWN

	//fmt.Println("======= Initating Server : ", myid, "==========")

	var clientPortAddr string
	for i := range obj.Servers {

		if obj.Servers[i].Id == strconv.Itoa(myid) {
			serverVar.Pid, _ = strconv.Atoi(obj.Servers[i].Id)
			clientPortAddr = obj.Servers[i].HostName + ":" + obj.Servers[i].ClientPort

		} else {

			serverVar.Peers[count], _ = strconv.Atoi(obj.Servers[i].Id)
			//fmt.Println("Peers -> ", serverVar.Peers[count])
			count++

		}
		//Assigning first ID as leader
		if LeaderID == -1 {
			LeaderID, _ = strconv.Atoi(obj.Servers[i].Id)

		}

		//fmt.Println("Server PID  = ", serverVar.Pid)
		serverVar.Path = obj.Path.Path

		servid, _ := strconv.Atoi(obj.Servers[i].Id)
		serverVar.Address[servid] = obj.Servers[i].HostName + ":" + obj.Servers[i].LogPort
		//fmt.Println("Server Address is ",serverVar.Address[servid],"servid = ",servid)

	}

	gob.Register(LogEntryStruct{})
	gob.Register(appendEntries{})
	gob.Register(appendEntriesResponse{})

	no_servers, _ := strconv.Atoi(obj.Count.Count)
	Quorum = int(no_servers/2 + 1.0)
	for i := range serverVar.PeersIds() {
		serverVar.LogSockets[serverVar.Peers[i]], _ = zmq.NewSocket(zmq.PUSH)
		serverVar.LogSockets[serverVar.Peers[i]].SetSndtimeo(time.Millisecond * 30)
		err := serverVar.LogSockets[serverVar.Peers[i]].Connect("tcp://" + serverVar.Address[serverVar.Peers[i]])
		//fmt.Println("Log Port : ", serverVar.LogSockets[serverVar.Peers[i]])
		if err != nil {
			panic("Connect error " + err.Error())
		}

		//initialize matchIndex
		serverVar.MatchIndex[serverVar.Peers[i]] = 0
		serverVar.NextIndex[serverVar.Peers[i]] = serverVar.GetLastApplied() + 1

		voteMap[serverVar.Peers[i]] = false

		//fmt.Println("serverVar.NextIndex[serverVar.Peers[i]] = ", serverVar.NextIndex[serverVar.Peers[i]])
	}
	// Fork methods for communication within cluster

	//fmt.Println("Leader = ", LeaderID)
	
	serverVar.SLog.ApplyFunc = func(e *LogEntryStruct) {
		serverVar.Inchan <- &(e.DataArray)
		}
	go SendMail(serverVar)
	go GetMail(serverVar)

	//Open port for client-server communication
	go serverVar.ClientServerComm(clientPortAddr)

	return serverVar
}

func SendMail(serverVar *Raft) {
	var network bytes.Buffer
	for {
		envelope := <-(serverVar.Outbox())

		if envelope.Pid == BROADCAST {
			envelope.Pid = serverVar.ServId()
			for i := range serverVar.PeersIds() {
				network.Reset()
				enc := gob.NewEncoder(&network)
				err := enc.Encode(envelope)
				if err != nil {
					panic("gob error: " + err.Error())
				}

				serverVar.LogSockets[serverVar.Peers[i]].Send(network.String(), 0)
			}

		} else {
			network.Reset()
			a := envelope.Pid
			envelope.Pid = serverVar.ServId()
			enc := gob.NewEncoder(&network)
			err := enc.Encode(envelope)
			if err != nil {
				panic("gob error: " + err.Error())
			}
			serverVar.LogSockets[a].Send(network.String(), 0)
		}

	}
}

func GetMail(ServerVar *Raft) {

	input, err := zmq.NewSocket(zmq.PULL)
	if err != nil {
		panic("Socket: " + err.Error())
	}
	err = input.Bind("tcp://" + ServerVar.Address[ServerVar.ServId()])
	if err != nil {
		panic("Socket: " + err.Error())
	}
	for {
		msg, err := input.Recv(0)

		if err != nil {
		}

		b := bytes.NewBufferString(msg)
		dec := gob.NewDecoder(b)

		env := new(Envelope)
		err = dec.Decode(env)
		if err != nil {
			log.Fatal("decode:", err)
		}

/*		if env.MessageId == 1 {

			go AppendEntriesRPC(ServerVar.ServId(), ServerVar)

		} else if env.MessageId == 2 {

			go ConfirmConsensus(ServerVar.ServId(), ServerVar)
		}*/
		(ServerVar.Inbox()) <- env

	}
}

// Function to check if consensus from majority is achieved or not
func ConfirmConsensus(Servid int, ServerVar *Raft,resp *appendEntriesResponse) {


	if resp.Success{

	env := <-(ServerVar.Inbox())

	les := env.Message.(LogEntryStruct)

	if _, exist := MsgAckMap[les.Lsn()]; exist {

		MsgAckMap[les.Lsn()]++

	} else {

		//Ignoring ACK since consensus already achieved
		return
	}

	if MsgAckMap[les.Lsn()] == Quorum {

		les.Commit <- true
		(ServerVar.GotConsensus) <- true
		CommitCh <- les
		delete(MsgAckMap, les.Lsn())
		MutexAppend.Unlock()
	}
	
	}
	return
}

//resetElectTo() resets the election ticker once it reaches timeout
func (ServerVar *Raft) resetElectTimeout() {
	ServerVar.ElectTicker = time.NewTimer(electTimeout()).C
}

//electTimeout() returns timeout duration for an election timer
func electTimeout() time.Duration {
	min := rand.Intn(int(MaxElectTo - MinElectTo))
	tm := int(MinElectTo) + min
	return time.Duration(tm) * time.Millisecond
}
