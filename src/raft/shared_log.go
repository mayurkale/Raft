package raft

import (
	
	"fmt"
	"strconv"
	"sync"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"	
	"github.com/syndtr/goleveldb/leveldb"
	
)

type ErrRedirect int // See Log.Append. Implements Error interface.

var MsgAckMap map[Lsn]int

var (
	errNoCommand       = errors.New("no command")
	errWrongIndex      = errors.New("bad index")
	errWrongTerm       = errors.New("bad term")
	errTermIsSmall     = errors.New("term is too small")
	errIndexIsSmall    = errors.New("index is too small")
	errIndexIsBig      = errors.New("commit index is too big")
	errChecksumInvalid = errors.New("checksum invalid")
)

type Log struct {
	sync.RWMutex
	ApplyFunc   func(*LogEntryStruct)
	db          *leveldb.DB
	entries     []*LogEntryStruct
	commitIndex uint64
	initialTerm uint64
}

// create new log
func createNewLog(dbPath string) *Log {
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		panic(fmt.Sprintf("dir not exist,%v", err))
	}
	l := &Log{
		entries:     []*LogEntryStruct{},
		db:          db,
		commitIndex: 0,
		initialTerm: 0,
	}
	l.FirstRead()
	return l
}

//Retrnt he current log index
func (l *Log) currentIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.CurrentIndexWithOutLock()
}

// The current index in the log without locking
func (l *Log) CurrentIndexWithOutLock() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return uint64(l.entries[len(l.entries)-1].Logsn)
}

// Closes the log file.
func (l *Log) close() {
	l.Lock()
	defer l.Unlock()
	l.db.Close()
	l.entries = make([]*LogEntryStruct, 0)
}

//Does log contains the retry with perticular index and term
func (l *Log) containsEntry(index uint64, term uint64) bool {
	entry := l.getEntry(index)
	return (entry != nil && uint64(entry.TermIndex) == term)
}

//get perticular entry by index
func (l *Log) getEntry(index uint64) *LogEntryStruct {
	l.RLock()
	defer l.RUnlock()
	if index <= 0 || index > (uint64(len(l.entries))) {
		return nil
	}
	return l.entries[index-1]
}

//read all enteries from disk when log intialized
func (l *Log) FirstRead() error {
	iter := l.db.NewIterator(nil, nil)
	count := 0
	for iter.Next() {
		count++
		entry := new(LogEntryStruct)
		value := iter.Value()
		b := bytes.NewBufferString(string(value))
		dec := gob.NewDecoder(b)
		err := dec.Decode(entry)
		if err != nil {
			panic(fmt.Sprintf("decode:", err))
		}
		if uint64(entry.Logsn) > 0 {
			// Append entry.
			l.entries = append(l.entries, entry)
			if uint64(entry.Logsn) <= l.commitIndex {
				l.ApplyFunc(entry)
			}
		}
	}
	iter.Release()
	err := iter.Error()
	return err
}

//It will return the entries after the given index
func (l *Log) entriesAfter(index uint64, maxLogEntriesPerRequest uint64) ([]*LogEntryStruct, uint64) {
	l.RLock()
	defer l.RUnlock()
	if index < 0 {
		return nil, 0
	}
	if index > (uint64(len(l.entries))) {
		panic(fmt.Sprintf("raft: Index is beyond end of log: %v %v", len(l.entries), index))
	}
	pos := 0
	lastTerm := uint64(0)
	
	for ; pos < len(l.entries); pos++ {
		if uint64(l.entries[pos].Logsn) > index {
			break
		}
		lastTerm = uint64(l.entries[pos].TermIndex)
	}
	a := l.entries[pos:]
	if len(a) == 0 {
		return []*LogEntryStruct{}, lastTerm
	}
	//if entries are less then max limit then return all entries
	if uint64(len(a)) < maxLogEntriesPerRequest {
		return closeResponseChannels(a), lastTerm
	} else {
		//otherwise return only max no of enteries premitted
		return a[:maxLogEntriesPerRequest], lastTerm
	}
}

//close the response channel of entries store on disk (leveldb)
func closeResponseChannels(a []*LogEntryStruct) []*LogEntryStruct {
	stripped := make([]*LogEntryStruct, len(a))
	for i, entry := range a {
		stripped[i] = &LogEntryStruct{
			Logsn:     entry.Logsn,
			TermIndex:      entry.TermIndex,
			DataArray:   entry.DataArray,
			Commit : nil,
		}
	}
	return stripped
}

//Return the last log entry term
func (l *Log) lastTerm() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.lastTermWithOutLock()
}
func (l *Log) lastTermWithOutLock() uint64 {
	if len(l.entries) <= 0 {
		return 0
	}
	return uint64(l.entries[len(l.entries)-1].TermIndex)
}

//Remove the enteries which are not commited
func (l *Log) discardEntries(index, term uint64) error {
	l.Lock()
	defer l.Unlock()
	if index > l.lastIndexWithOutLock() {
		return errIndexIsBig
	}
	if index < l.getCommitIndexWithOutLock() {
		return errIndexIsSmall
	}
	if index == 0 {
		for pos := 0; pos < len(l.entries); pos++ {
			if l.entries[pos].Commit != nil {
				l.entries[pos].Commit <- false
				close(l.entries[pos].Commit)
				l.entries[pos].Commit = nil
			}
		}
		l.entries = []*LogEntryStruct{}
		return nil
	} else {
		// Do not discard if the entry at index does not have the matching term.
		entry := l.entries[index-1]
		if len(l.entries) > 0 && uint64(entry.TermIndex) != term {
			return errors.New(fmt.Sprintf("raft.Log: Entry at index does not have matching term (%v): (IDX=%v, TERM=%v)", entry.TermIndex, index, term))
		}
		// Otherwise discard up to the desired entry.
		
		if index < uint64(len(l.entries)) {
			buf := make([]byte, 8)
			// notify clients if this node is the previous leader
			for i := index; i < uint64(len(l.entries)); i++ {
				entry := l.entries[i]
				binary.LittleEndian.PutUint64(buf, uint64(entry.Logsn))
				err := l.db.Delete(buf, nil)
				if err != nil {
					panic("entry not exist")
				}
				if entry.Commit != nil {
					entry.Commit <- false
					close(entry.Commit)
					entry.Commit = nil
				}
			}
			l.entries = l.entries[0:index]
		}
	}
	return nil
}

//Return lastest commit index
func (l *Log) getCommitIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.getCommitIndexWithOutLock()
}
func (l *Log) getCommitIndexWithOutLock() uint64 {
	return l.commitIndex
}

//Return lastlog entry index
func (l *Log) lastIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.lastIndexWithOutLock()
}
func (l *Log) lastIndexWithOutLock() uint64 {
	if len(l.entries) <= 0 {
		return 0
	}
	return uint64(l.entries[len(l.entries)-1].Logsn)
}

// Appends a series of entries to the log.
func (l *Log) appendEntries(entries []*LogEntryStruct) error {
	l.Lock()
	defer l.Unlock()
	// Append each entry but exit if we hit an error.
	for i := range entries {
		if err := entries[i].writeToDB(l.db); err != nil {
			return err
		} else {
			l.entries = append(l.entries, entries[i])
		}
	}
	return nil
}

// Append entry will append entry into in-memory log as well as will write on disk(for us leveldb)
func (l *Log) appendEntry(entry *LogEntryStruct) error {
	l.Lock()
	defer l.Unlock()
	if len(l.entries) > 0 {
		lastTerm := l.lastTermWithOutLock()
		if uint64(entry.TermIndex) < lastTerm {
			return errTermIsSmall
		}
		lastIndex := l.lastIndexWithOutLock()
		if uint64(entry.TermIndex) == lastTerm && uint64(entry.Logsn) <= lastIndex {
			return errIndexIsSmall
		}
	}
	if err := entry.writeToDB(l.db); err != nil {
		return err
	}
	l.entries = append(l.entries, entry)
	return nil
}

//Update commit index
func (l *Log) updateCommitIndex(index uint64) {
	l.Lock()
	defer l.Unlock()
	if index > l.commitIndex {
		l.commitIndex = index
	}
}

//Commit current log to given index
func (l *Log) commitTill(commitIndex uint64) error {
	l.Lock()
	defer l.Unlock()
	if commitIndex > uint64(len(l.entries)) {
		commitIndex = uint64(len(l.entries))
	}
	if commitIndex < l.commitIndex {
		return nil
	}
	pos := l.commitIndex + 1
	if pos < 0 {
		panic("pending commit pos < 0")
	}
	for i := l.commitIndex + 1; i <= commitIndex; i++ {
		entryIndex := i - 1
		entry := l.entries[entryIndex]
		// Update commit index.
		l.commitIndex = uint64(entry.Logsn)
		if entry.Commit != nil {
			entry.Commit <- true
			close(entry.Commit)
			entry.Commit = nil
		} else {
			//Give entry to state machine to apply
			l.ApplyFunc(entry)
		}
	}
	return nil
}

//Get last commit information
func (l *Log) commitInfo() (index uint64, term uint64) {
	l.RLock()
	defer l.RUnlock()
	if l.commitIndex == 0 {
		return 0, 0
	}
	if l.commitIndex == 0 {
		return 0, 0
	}
	entry := l.entries[l.commitIndex-1]
	return uint64(entry.Logsn), uint64(entry.TermIndex)
}

//Write entry to leveldb
func (e *LogEntryStruct) writeToDB(db *leveldb.DB) error {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	err := enc.Encode(e)
	if err != nil {
		panic("gob error: " + err.Error())
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(e.Logsn))
	err = db.Put(buf, []byte(network.String()), nil)
	return err
}

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

func (les LogEntryStruct) Term() uint64 {
	return les.TermIndex
}

func (les LogEntryStruct) Lsn() Lsn {
	return les.Logsn
}

func (les LogEntryStruct) Data() []byte {
	return les.DataArray
}

func (les LogEntryStruct) Committed() bool {
	return <- les.Commit
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
	les.Commit = nil

	var msg appendEntries

	msg.TermIndex = ServerVar.Term

	msg.Entries = append(msg.Entries, &les)

	//fmt.Println("Server...........  2------",len(msg.Entries),msg.Entries[0].Logsn)

	var envVar Envelope
	envVar.Pid = BROADCAST
	//TODO
	envVar.MessageId = APPENDENTRIESRPC //APPRIESRPC
	envVar.SenderId = ServerVar.ServId()
	envVar.LastLogIndex = ServerVar.GetPrevLogIndex()
	envVar.LastLogTerm = ServerVar.GetPrevLogTerm()
	envVar.Message = msg
	MsgAckMap[les.Lsn()] = 1
	ServerVar.Outbox() <- &envVar
	//fmt.Println("Server...........  1")

	//nil means everything was fine in append and it has done all the expected work.
	return les, nil
}
