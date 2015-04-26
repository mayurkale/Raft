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
	"os"
	"sort"
	"strconv"
	"time"
)


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

	sendHeartBeats(ni *nextIndex, timeout time.Duration) (int, bool)

	handleAppendEntries(env *Envelope) (*appendEntriesResponse, bool)

	ApplyCommandToSM()
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

func (ServerVar *Raft) catchUpLog(ni *nextIndex, id int, timeout time.Duration) error {

	currentTerm := ServerVar.Term

	prevLogIndex := ni.prevLogIndex(uint64(id)) //.........

	resultentries, prevLogTerm := ServerVar.SLog.entriesAfter(prevLogIndex, 10)

	//commitIndex := ServerVar.SLog.getCommitIndex()

	var envVar Envelope
	envVar.Pid = id
	envVar.MessageId = APPENDENTRIES
	envVar.SenderId = ServerVar.ServId()
	envVar.Leaderid = ServerVar.LeaderId
	envVar.LastLogIndex = prevLogIndex
	envVar.LastLogTerm = prevLogTerm
	envVar.CommitIndex = ServerVar.SLog.commitIndex
	
//	fmt.Println("APPENDENTRIES sending lsn = ",envVar.LastLsn,"for server = ",id)	
	
	envVar.Message = &appendEntries{TermIndex: currentTerm, Entries: resultentries}
	if debug {
	fmt.Println("Server ", ServerVar.ServId(), "->", id, " Prev log= ", prevLogIndex, "Prev Term = ", prevLogTerm)
}
	ServerVar.Outbox() <- &envVar

	select {

	//case <-replication:

	case env := <-(ServerVar.Inbox()):
		switch env.MessageId {

		case APPENDENTRIESRESPONSE:
	
	id = env.SenderId
	prevLogIndex = ni.prevLogIndex(uint64(id)) //.........

	resultentries, prevLogTerm = ServerVar.SLog.entriesAfter(prevLogIndex, 10)
	
	
	
//		fmt.Println("APPENDENTRIESRESPONSE received lsn = ",LastLsn,"for server = ",env.SenderId)
		

			if debug {

				fmt.Println("Received APPENDENTRIESRESPONSE at RIGHT place")
			}
			
			resp := env.Message.(appendEntriesResponse)
			if resp.Term > currentTerm {
			
//			fmt.Println("Term not matched hence returing from APPENDENTRIES RESPONSE")
				return errorDeposed
			}

			if !resp.Success {

				newPrevLogIndex, err := ni.decrement(uint64(id), prevLogIndex)
				if debug {
				fmt.Println("No SUCC: new index for ", id, "is", newPrevLogIndex, "where prev log index was ", prevLogIndex)
				}
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
				if debug {
				fmt.Println("SET : new prev index for ", id, "is", newPrevLogIndex,"Term  = ",resp.Term)
				}
				if err != nil {

					return err
				}

				return nil
			} /*else {
				fmt.Println("NOT SET : new prev index for id =", id,"sender = ",env.SenderId,"Term  = ",resp.Term)
			}*/

			return nil
		}
	case <-time.After(2 * timeout):
		return errorTimeout
	}
	return nil
}

func (ServerVar *Raft) handleAppendEntries(env *Envelope) (*appendEntriesResponse, bool) {

	if env == nil {

		fmt.Println("Eureka..............")
	}

	resp := env.Message.(appendEntries)
	if debug {
	fmt.Println("handleAppendEntries() : Fo server ", ServerVar.ServId(), " Term : ", ServerVar.Term, " env Term = ", resp.TermIndex, "Commit index ", env.CommitIndex, " ServerVar.SLog.commitIndex ", ServerVar.SLog.commitIndex)
}

	if resp.TermIndex < ServerVar.Term {
//		fmt.Println("Giving false here.....1")
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

	//	fmt.Println("Server ",ServerVar.ServId(),"Discard ",env.LastLogIndex,env.LastLogTerm," - log ",ServerVar.SLog.lastIndex(),ServerVar.SLog.lastTerm())
	if err := ServerVar.SLog.discardEntries(env.LastLogIndex, env.LastLogTerm); err != nil {

//		fmt.Println("Giving false here.....2", err)
		return &appendEntriesResponse{
			Term:    ServerVar.Term,
			Success: false,
			reason:  fmt.Sprintf("while ensuring last log entry had index=%d term=%d: error: %s", env.LastLogIndex, env.LastLogTerm, err)}, downGrade
	}

	//resp := env.Message.(appendEntries)

	for i, entry := range resp.Entries {

		//			fmt.Println("Appending entry for server ",ServerVar.ServId())
		if err := ServerVar.SLog.appendEntry(entry); err != nil {
//			fmt.Println("Giving false here.....3")
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
		if debug {
		fmt.Println("handle() Server ", ServerVar.ServId(), " appendEntry")
}
	}

	if env.CommitIndex > 0 && env.CommitIndex > ServerVar.SLog.commitIndex {

		if err := ServerVar.SLog.commitTill(env.CommitIndex); err != nil {

	//		fmt.Println("Giving false here.....4")

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

//Below function is used to set new index values to all the peers
func (ServerVar *Raft) newNextIndex(defaultNextIndex uint64) *nextIndex {
	ni := &nextIndex{
		m: map[uint64]uint64{},
	}
	for _, id := range ServerVar.PeersIds() {
		ni.m[uint64(id)] = defaultNextIndex
		
		//fmt.Println("INITIALIZEDn ", id, " to ", ni.m[uint64(id)])
	}
	return ni
}


//Returns the previous log index of a server whose id passed as argument
func (ni *nextIndex) prevLogIndex(id uint64) uint64 {
	ni.RLock()
	defer ni.RUnlock()
	if _, ok := ni.m[id]; !ok {
		panic(fmt.Sprintf("peer %d not found", id))
	}
	return ni.m[id]
}


//Decrements lastlog index by 1
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
if debug {

		fmt.Println("decremented val ", ni.m[id])
}
	}
	return ni.m[id], nil
}

//Sets prev log index to index ,for a server if previous log index matches with argument
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

if debug {
	fmt.Println("set val ", ni.m[id])
}
	return index, nil
}

//This function is used to send concurrent heartbeat messages to all the peers, and then collects and returns number of positive responses  
func (ServerVar *Raft) sendHeartBeats(ni *nextIndex, timeout time.Duration) (int, bool) {

	type tuple struct {
		id  uint64
		err error
	}
	responses := make(chan tuple, len(ServerVar.PeersIds()))
	for _, id1 := range ServerVar.PeersIds() {
		go func(id int) {
			errChan := make(chan error, 1)
			go func() { errChan <- ServerVar.catchUpLog(ni, id, timeout) }()
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

			cmd := t.(*CommandTuple)
			// Append the command to our (leader) log
	
	if debug {		fmt.Println("got command, appending", ServerVar.Term)
	
	}
			currentTerm := ServerVar.Term
			comma := new(bytes.Buffer)
			encCommand := gob.NewEncoder(comma)
			encCommand.Encode(cmd)
			entry := &LogEntryStruct{
				Logsn:     Lsn(ServerVar.SLog.lastIndex() + 1),
				TermIndex: currentTerm,
				DataArray: cmd.Com, //comma.Bytes(),
				Commit:    cmd.ComResponse,
			}

			if err := ServerVar.SLog.appendEntry(entry); err != nil {
				panic(err)
				continue
			}
if debug {

			fmt.Printf(" Leader after append, commitIndex=%d lastIndex=%d lastTerm=%d", ServerVar.SLog.getCommitIndex(), ServerVar.SLog.lastIndex(), ServerVar.SLog.lastTerm())
}
			go func() {
//				fmt.Println("sending replicate")
				replicate <- struct{}{}
			}()

		case <-replicate:
//			fmt.Println("HBT")
			successes, downGrade := ServerVar.sendHeartBeats(nIndex, 2*heartBeatInterval())

			if downGrade {

				//As leader downgrade will result in having unknown leader and going into follower state
				ServerVar.LeaderId = UNKNOWN
				ServerVar.State = FOLLOWER

				return
			}
//			fmt.Println("Successes ---> ", successes)
			if successes >= Quorum-1 {

				var indices []uint64
				indices = append(indices, ServerVar.SLog.currentIndex())
				for _, i := range nIndex.m {
					indices = append(indices, i)
				}
				sort.Sort(uint64Slice(indices))

				commitIndex := indices[Quorum-1]

				committedIndex := ServerVar.SLog.commitIndex

				peersBestIndex := commitIndex

				ourLastIndex := ServerVar.SLog.lastIndex()

				ourCommitIndex := ServerVar.SLog.getCommitIndex()

				
				if peersBestIndex > ourLastIndex {
					ServerVar.LeaderId = UNKNOWN
					ServerVar.VotedFor = NOVOTE
					ServerVar.State = FOLLOWER
					return
				}

				if commitIndex > committedIndex {
					// leader needs to do a sync before committing log entries
					if err := ServerVar.SLog.commitTill(peersBestIndex); err != nil {

						continue
					}
					if ServerVar.SLog.getCommitIndex() > ourCommitIndex {
						go func() { replicate <- struct{}{} }()
					}
				}

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
				if debug {
				fmt.Println("Received APPENDENTRIES for ", ServerVar.ServId())
				}
				resp, down := ServerVar.handleAppendEntries(env)


				var envVar Envelope
				envVar.Pid = env.Leaderid
				envVar.MessageId = APPENDENTRIESRESPONSE
				envVar.SenderId = ServerVar.ServId()
				envVar.Leaderid = ServerVar.LeaderId
				envVar.LastLogIndex = env.LastLogIndex
				envVar.LastLogTerm = env.LastLogTerm
				envVar.CommitIndex = env.CommitIndex
				//envVar.LastLsn = env.LastLsn
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

			
			
			
			}
		}

	}

}

func heartBeatInterval() time.Duration {
	tm := MinElectTo / 6
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

				vcount := 1
				for i := range ServerVar.PeersIds() {
					if voteMap[ServerVar.Peers[i]] == true {
						vcount++
					}

				}
				if debug {
					fmt.Println(" Candidate Server id = ", ServerVar.ServId(), " vcount = ", vcount, " Quorum = ", Quorum)

				}

				if vcount >= (Quorum) {
					ServerVar.LeaderId = ServerVar.ServId()
					ServerVar.State = LEADER
					ServerVar.VotedFor = NOVOTE
					if debug {
						fmt.Println(" New Leader Server id = ", ServerVar.ServId())

					}

					return
				}

			case APPENDENTRIES:
				if debug {
				fmt.Println("Received APPENDENTRIES for ", ServerVar.ServId())
				}
				resp, down := ServerVar.handleAppendEntries(env)

				var envVar Envelope
				envVar.Pid = env.Leaderid
				envVar.MessageId = APPENDENTRIESRESPONSE
				envVar.SenderId = ServerVar.ServId()
				envVar.Leaderid = ServerVar.LeaderId
				envVar.LastLogIndex = env.LastLogIndex
				envVar.LastLogTerm = env.LastLogTerm
				envVar.CommitIndex = env.CommitIndex
				//envVar.LastLsn = env.LastLsn
				envVar.Message = resp
				ServerVar.Outbox() <- &envVar

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

	//(*ServerVar).LsnVar = (*ServerVar).LsnVar + 1
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
				//fmt.Println("Received APPENDENTRIES for ", ServerVar.ServId())
				if ServerVar.LeaderId == UNKNOWN {
					ServerVar.LeaderId = env.SenderId

				}

				resp, down := ServerVar.handleAppendEntries(env)

				//fmt.Println("FOLLOWER: B4 Sending returned from handle ",ServerVar.ServId())

				var envVar Envelope
				envVar.Pid = env.Leaderid
				envVar.MessageId = APPENDENTRIESRESPONSE
				envVar.SenderId = ServerVar.ServId()
				envVar.Leaderid = ServerVar.LeaderId
				envVar.LastLogIndex = env.LastLogIndex
				envVar.LastLogTerm = env.LastLogTerm
				envVar.CommitIndex = env.CommitIndex
				//envVar.LastLsn = env.LastLsn
				envVar.Message = resp

				ServerVar.Outbox() <- &envVar
				//fmt.Println("FOLLOWER: After Sending ",ServerVar.ServId())
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

	//(*serverVar).LsnVar = (*serverVar).LsnVar + 1
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
		Inchan:        make(chan *LogEntryStruct),
		Outchan:       make(chan interface{}),
		ClusterSize:   len(obj.Servers) - 1,
		GotConsensus:  make(chan bool),
		SLog:          tLog,
		Inprocess:     false,
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
	gob.Register(Command{})

	no_servers, _ := strconv.Atoi(obj.Count.Count)
	Quorum = int((no_servers-1)/2 + 1.0)
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

		//fmt.Println("ApplyFunc() --> ", e.Logsn)
		serverVar.Inchan <- e
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

		
		(ServerVar.Inbox()) <- env

	}
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
