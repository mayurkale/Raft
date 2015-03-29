package raft

import (
	"fmt"
	"strconv"
)

type ErrRedirect int // See Log.Append. Implements Error interface.

var MsgAckMap map[Lsn]int

type SharedLog interface {
	// Each data item is wrapped in a LogEntry with a unique
	// lsn. The only error that will be returned is ErrRedirect,
	// to indicate the server id of the leader. Append initiates
	// a local disk write and a broadcast to the other replicas,
	// and returns without waiting for the result.

	ClusterComm()
	Append(data []byte) (LogEntry, error)
}

//Envelop packs message along with server id and message id.
/* MessageId denotes what type of message it is
1 => Call need to be made AppendEntryRPC()
2 => Message is ACK from Peers

*/

type Envelope struct {
	Pid          int
	SenderId     int
	MessageId    int
	LastLogIndex uint64
	LastLogTerm  int
	Message      LogEntryStruct
}

type LogEntry interface {
	Lsn() Lsn
	Data() []byte
	Term() int
	Committed() bool
}

type LogEntryStruct struct {
	Logsn     Lsn
	TermIndex int
	DataArray []byte
	Commit    bool
}

func (les LogEntryStruct) Term() int {
	return les.TermIndex
}

func (les LogEntryStruct) Lsn() Lsn {
	return les.Logsn
}

func (les LogEntryStruct) Data() []byte {
	return les.DataArray
}

func (les LogEntryStruct) Committed() bool {
	return les.Commit
}

func (e ErrRedirect) Error() string {

	return "Redirect to server " + strconv.Itoa(int(e)) + "\r\n"
}

func AppendEntriesRPC(Servid int, ServerVar *Raft) {

	env := <-(ServerVar.Inbox())

	env.Pid = env.SenderId
	env.MessageId = CONFIRMCONSENSUS
	env.SenderId = Servid

	ServerVar.Outbox() <- env

	//nil means everything was fine in append and it has done all the expected work.

}

//Append() Will first check if current server is leader, if not it'll return an error
//else it'll broadcast to peers in cluster a request for consensus.

func (ServerVar *Raft) Append(data []byte) (LogEntry, error) {

	var LogEnt LogEntry

	var err ErrRedirect
	if ServerVar.GetState() != LEADER {
		err = ErrRedirect(ServerVar.GetLeader())

		fmt.Println("THis is not leader ", ServerVar.ServId())
		fmt.Println("GetLeader  = ", ServerVar.GetLeader())
		MutexAppend.Unlock()
		//unlock the MutexAppend Call lock
		return LogEnt, err
	}

	fmt.Println("THis seems leader ", ServerVar.ServId())
	fmt.Println("GetLeader  = ", ServerVar.GetLeader())
	fmt.Println("GetState  = ", ServerVar.GetState())

	var les LogEntryStruct

	(*ServerVar).LsnVar = (*ServerVar).LsnVar + 1
	les.Logsn = Lsn((*ServerVar).LsnVar)
	les.DataArray = data
	les.TermIndex = ServerVar.GetTerm()
	les.Commit = false

	var envVar Envelope
	envVar.Pid = BROADCAST
	envVar.MessageId = APPENDENTRIESRPC
	envVar.SenderId = ServerVar.ServId()
	envVar.LastLogIndex = ServerVar.GetPrevLogIndex()
	envVar.LastLogTerm = ServerVar.GetPrevLogTerm()
	envVar.Message = les
	MsgAckMap[les.Lsn()] = 1
	ServerVar.Outbox() <- &envVar
	//nil means everything was fine in append and it has done all the expected work.
	return les, nil
}
