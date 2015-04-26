package raft

import (
	"errors"
	zmq "github.com/pebbe/zmq4"
	"github.com/syndtr/goleveldb/leveldb"
	"net"
	"sync"
	"time"
)

type ErrRedirect int // See Log.Append. Implements Error interface.

var MsgAckMap map[Lsn]int

//Map to maintain log-entry to client conn mapping, used while sending back response to client
var LogEntMap map[Lsn]net.Conn

//CommitCh channel is used by sharedlog, to put commited Command entry onto channel for kvstore to execute
var CommitCh chan LogEntryStruct

//Map for Key value store
var keyval = make(map[string]valstruct)

//Locks Used
var mutex = &sync.RWMutex{}    //Lock for keyval store
var MutexLog = &sync.RWMutex{} //Lock for LogEntMap store

type PriorityQueue []*Item

//Below constants are used to set message IDs.
const (
	BROADCAST = -1

	NOVOTE         = -1
	UNKNOWN        = -1
	INITIALIZATION = 0

	APPENDENTRIESRPC         = 1
	CONFIRMCONSENSUS         = 2
	REQUESTVOTE              = 3
	VOTERESPONSE             = 4
	APPENDENTRIES            = 5
	APPENDENTRIESRESPONSE    = 6
	HEARTBEATRESPONSE        = 7
	HEARTBEAT                = 8
	APPENDENTRIESRPCRESPONSE = 9

	FOLLOWER  = 1
	CANDIDATE = 2
	LEADER    = 3
)

var (

	//Below are various error description
	errNoCommand = errors.New("NO COMMAND")
	errorTimeout = errors.New("TIMEOUT")

	errWrongIndex              = errors.New("BAD INDEX")
	errWrongTerm               = errors.New("BAD TERM")
	errTermIsSmall             = errors.New("TOO_SMALL_TERM")
	errorappendEntriesRejected = errors.New("APPENDENTRIES_REJECTED")
	errIndexIsSmall            = errors.New("TOO_SMALL_INDEX")
	errIndexIsBig              = errors.New("TOO_BIG_COMMIT_INDEX")
	errorDeposed               = errors.New("DEPOSED")
	errorOutOfSync             = errors.New("OUTOFSYNC")

	errChecksumInvalid = errors.New("INVALID CHECKSUM")

	//Lock for vote counting
	voteLock = &sync.RWMutex{}

	voteMap = make(map[int]bool)

	/*
	   Both candidate and follower state raft server will wait for random time between min and max election timeout
	   and once it's wait is over, will start election all over again.
	*/
	MinElectTo int32 = 200
	MaxElectTo       = 3 * MinElectTo
)

type Log struct {
	sync.RWMutex
	ApplyFunc   func(*LogEntryStruct)
	db          *leveldb.DB
	entries     []*LogEntryStruct
	commitIndex uint64
	initialTerm uint64
}

var debug = false

type Lsn uint64 //Log sequence number, unique for all time.

//LeaderID denotes id of the leader
var LeaderID, Quorum int

type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] > p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }



type Envelope struct {
	Pid          int
	SenderId     int
	Leaderid     int
	MessageId    int
	CommitIndex  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
	
	
	//Message      LogEntryStruct
	Message interface{}
}

type LogEntry interface {
	Lsn() Lsn
	Data() []byte
	Term() uint64
	Committed() bool
}

type LogEntryStruct struct {
	Logsn     Lsn
	TermIndex uint64
	DataArray []byte
	Commit    chan bool
}

//Below struct is used to envelop coomand inside it.
type CommandTuple struct {
	Com         []byte
	ComResponse chan bool
	Err         chan error
}




//Structure to store the value for each key
type valstruct struct {
	version    int64
	expirytime int
	timestamp  int64
	numbytes   int
	value      []byte
}

//Structure to store the parsed command sent by the client to connhandler
type Command struct {
	CmdType    int // 1-set , 2-cas, 3-get , 4-getm , 5-delete
	Key        string
	Expirytime int
	Len        int
	Value      []byte
	Version    int64
}

//Heap(Priority Queue Implementation) -- code functions taken from Golang ducumentation examples and edited
var PQ = make(PriorityQueue, 0)

type Item struct {
	value     string // The value of the item; arbitrary.
	priority  int64  // The priority of the item in the queue.
	timestamp int64  // To ensure unique element insert in heap (helps in deleting/updating elements)
	index     int    // The index of the item in the heap.
}

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

//store index of all peer servers
type nextIndex struct {
	sync.RWMutex
	m map[uint64]uint64 // followerId: nextIndex
}

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

//Raft is a structure that defines server and also maintains information about cluster.

type Raft struct {
	Pid           int
	Peers         []int
	Path          string
	Term          uint64 // Current term ID for raft object.(Monotonic increase)
	VotedFor      int    // Server ID for which this raft has voted in leader election.
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
	Inchan        chan *LogEntryStruct
	Outchan       chan interface{}
	Inprocess     bool
}
