package raft

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"io/ioutil"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
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
	APPENDENTRIESRPC = 1
	CONFIRMCONSENSUS = 2
	REQUESTVOTE      = 3
	VOTERESPONSE     = 4
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
	MaxElectTo       = 2 * MinElectTo
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
	Term          int            // Current term ID for raft object.(Monotonic increase)
	VotedFor      int            // Server ID for which this raft has voted in leader election.
	LeaderId      int            //Current Leader ID
	CommitIndex   uint64         //Index of the highest entry commited till time.(Monotonic increase)
	MatchIndex    map[int]uint64 //Index fo the highest log entry known to be replicated on server for all peers.
	NextIndex     map[int]uint64 //Index of the next log entry to send to that server for all peers (initialized to leader last log index + 1)
	PrevLogIndex  uint64
	PrevLogTerm   int
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
}

//Server interface declares functions that will be used to provide APIs to comminicate with server.
type Server interface {
	ServId() int

	PeersIds() []int

	GetTerm() int //Returns current term for the shared log for this raft server.

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

	GetPrevLogIndex() uint64

	GetPrevLogTerm() int

	SetTerm(int)

	SetVotedFor(int)

	requestForVoteToPeers()
}

func (ServerVar *Raft) Start() {

	ServerVar.loop()

}

func (ServerVar *Raft) SetTerm(term int) {
	ServerVar.Term = term

}

func (ServerVar *Raft) GetPrevLogIndex() uint64 {
	return (*ServerVar).PrevLogIndex
}

func (ServerVar *Raft) GetPrevLogTerm() int {
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

func (ServerVar *Raft) GetTerm() int {
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
			/*
				case LEADER:
					state = ServerVar.leader()
			*/
		default:
			return
		}
	}
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

			les := LogEntryStruct(env.Message)

			switch env.MessageId {

			case REQUESTVOTE:

				if debug {
					fmt.Println("Received request vote for candidate....ignore")
				}

				//Since any candidate would have already voted for self, we will ignore voting requests
				break

			case VOTERESPONSE:

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
	lesn.Commit = false

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

			les := LogEntryStruct(env.Message)

			switch env.MessageId {

			case REQUESTVOTE:

				if les.Term() < ServerVar.GetTerm() {
					//Do not send response
					//ServerVar.resetElectTimeout()
					break
				}

				if les.Term() > ServerVar.GetTerm() {

					ServerVar.Term = les.Term()
					ServerVar.VotedFor = NOVOTE
					ServerVar.LeaderId = UNKNOWN

				}

				if ServerVar.VotedFor != NOVOTE && ServerVar.VotedFor != env.SenderId {
					//Do not send response
					break
				}

				if ServerVar.GetPrevLogIndex() > env.LastLogIndex || ServerVar.PrevLogTerm > env.LastLogTerm {
					//Do not send response
					break
				}

				if ServerVar.GetVotedFor() == NOVOTE {

					var lesn LogEntryStruct

					(*ServerVar).LsnVar = (*ServerVar).LsnVar + 1
					lesn.Logsn = Lsn((*ServerVar).LsnVar)
					lesn.DataArray = nil
					lesn.TermIndex = ServerVar.GetTerm()
					lesn.Commit = false

					var envVar Envelope
					envVar.Pid = env.SenderId
					envVar.MessageId = VOTERESPONSE
					envVar.SenderId = ServerVar.ServId()
					envVar.LastLogIndex = ServerVar.GetPrevLogIndex()
					envVar.LastLogTerm = ServerVar.GetPrevLogTerm()
					envVar.Message = lesn
					//TODO: Whats below line??
					//MsgAckMap[les.Lsn()] = 1

					if debug {

						fmt.Println("Sending vote for Candidate=", env.SenderId, " Term = ", lesn.TermIndex, " follower = ", envVar.SenderId)
					}

					ServerVar.VotedFor = env.SenderId
					ServerVar.resetElectTimeout()
					ServerVar.Outbox() <- &envVar

				}

			}

		}
	}

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

	serverVar := &Raft{
		Pid:           UNKNOWN,
		Peers:         make([]int, len(obj.Servers)-1),
		Path:          "",
		Term:          INITIALIZATION,
		VotedFor:      NOVOTE,
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
		ClusterSize:   len(obj.Servers) - 1,
		GotConsensus:  make(chan bool),
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

		if env.MessageId == 1 {

			go AppendEntriesRPC(ServerVar.ServId(), ServerVar)

		} else if env.MessageId == 2 {

			go ConfirmConsensus(ServerVar.ServId(), ServerVar)
		}
		(ServerVar.Inbox()) <- env

	}
}

// Function to check if consensus from majority is achieved or not
func ConfirmConsensus(Servid int, ServerVar *Raft) {
	env := <-(ServerVar.Inbox())

	les := LogEntryStruct(env.Message)

	if _, exist := MsgAckMap[les.Lsn()]; exist {

		MsgAckMap[les.Lsn()]++

	} else {

		//Ignoring ACK since consensus already achieved
		return
	}

	if MsgAckMap[les.Lsn()] == Quorum {

		les.Commit = true
		(ServerVar.GotConsensus) <- true
		CommitCh <- les
		delete(MsgAckMap, les.Lsn())
		MutexAppend.Unlock()
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
