package raft

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	//"time"
)

//var AtomicAppend chan int
var MutexAppend = &sync.RWMutex{}

//ClientServerComm() is used to listen at a port through whichs clients are communication to servers.

type Keyvalstore interface {
	parseSetCas(cmdtype int, res []string, conn net.Conn)

	parseRest(cmdtype int, res []string, conn net.Conn)
	AppendUtility(conn net.Conn, command *bytes.Buffer, commandData *Command)

	ConnHandler(conn net.Conn)
}

func (ServerVar *Raft) ClientServerComm(clientPortAddr string) {

	//fmt.Println("Client Port : ", clientPortAddr)
	lis, error := net.Listen("tcp", clientPortAddr)
	defer lis.Close()

	if error != nil {
		panic("Client-Server connection error : " + error.Error())
	}
	//fmt.Println("State machine Apply ----- : ", ServerVar.ServId())
	go ServerVar.ApplyCommandToSM()
	go KvReadCommitCh()
	for {

		con, error := lis.Accept()
		if error != nil {
			panic("Client-Server accept error : " + error.Error())
			continue
		}

		go ServerVar.ConnHandler(con)

	}

}

func (ServerVar *Raft) parseSetCas(cmdtype int, res []string, conn net.Conn, reader *bufio.Reader) {

	if cmdtype == 1 && ((len(res) != 4) || len(res[1]) > 250) {
		returnmsg := "ERRCMDERR\r\n"
		conn.Write([]byte(returnmsg))
	} else if cmdtype == 2 && ((len(res) != 5) || len(res[1]) > 250) {
		returnmsg := "ERRCMDERR\r\n"
		conn.Write([]byte(returnmsg))
	} else {
		exptime, err1 := strconv.Atoi(res[2])

		var numbytes int
		var version int64
		var err2, err3 error

		if cmdtype == 1 {

			version = -1
			err2 = nil
			numbytes, err3 = strconv.Atoi(res[3])

		} else {

			version, err2 = strconv.ParseInt(res[3], 10, 64)
			numbytes, err3 = strconv.Atoi(res[4])
		}

		//......Assuming number of bytes of value will never be less than 0....
		if err1 != nil || err2 != nil || err3 != nil || exptime < 0 || numbytes < 0 {
			returnmsg := "ERRCMDERR\r\n"
			conn.Write([]byte(returnmsg))
			return
		}

		//.......Reading the value to store against the key.....
		valuebyt, err := reader.ReadBytes('\n')
		if err != nil {
			returnmsg := "ERR_INTERNAL\r\n"
			conn.Write([]byte(returnmsg))
			return
		}

		valuebytes := string(valuebyt)

		valuebytes = strings.TrimSpace(valuebytes)
		//....Checking if size of value read matches given value or not, else give error
		if len(valuebytes) != numbytes {

			returnmsg := "ERRCMDERR\r\n"
			conn.Write([]byte(returnmsg))
			return
		}

		commandData := Command{cmdtype, res[1], exptime, numbytes, ([]byte(valuebytes)), version}

		command := new(bytes.Buffer)
		encCommand := gob.NewEncoder(command)
		encCommand.Encode(commandData)

		//MutexAppend.Lock()
		ServerVar.Inprocess = true
		ServerVar.AppendUtility(conn, command, commandData)

	}

}

//AppendUtility() calls append() periodically after every 3 seconds for infinite time until consensus is reached among servers.
// Once concenus is arrived,it will return.
func (ServerVar *Raft) AppendUtility(conn net.Conn, command *bytes.Buffer, commandData Command) {
	//var LogEnt LogEntry

	if debug {
		fmt.Println("SERVERINFO: I am ", ServerVar.ServId(), "And leader is ", ServerVar.GetLeader())
	}
	
	
	
	var err ErrRedirect
	if ServerVar.GetState() != LEADER {

		//fmt.Println("Last Index for log is ", ServerVar.SLog.lastIndex())
		err = ErrRedirect(ServerVar.GetLeader())

		returnmsg := err.Error()
		conn.Write([]byte(returnmsg))
		return

	}
	//fmt.Println("Append Utility() SERVERINFO: I am ", ServerVar.ServId(), "And leader is ", ServerVar.GetLeader())
	//fmt.Println("Append Utility() Sending commands ",commandData.CmdType,"key = ",commandData.Key)

	//fmt.Println("Append() This seems leader ", ServerVar.ServId())

	var les LogEntryStruct
	//DO i need +1??
	//(*ServerVar).LsnVar = (*ServerVar).LsnVar + 1
	les.Logsn = Lsn(ServerVar.SLog.lastIndex() + 1)
	les.DataArray = command.Bytes()
	les.TermIndex = ServerVar.GetTerm()
	les.Commit = nil

	var msg appendEntries

	msg.TermIndex = ServerVar.Term

	msg.Entries = append(msg.Entries, &les)

	//fmt.Println("appendutility() Term index -----> ",ServerVar.Term)

	var envVar Envelope
	envVar.Pid = BROADCAST
	//TODO
	envVar.MessageId = APPENDENTRIES //APPRIESRPC
	envVar.Leaderid = ServerVar.LeaderId
	envVar.SenderId = ServerVar.ServId()
	envVar.LastLogIndex = ServerVar.SLog.lastIndex()
	envVar.LastLogTerm = ServerVar.SLog.lastTerm()
	envVar.CommitIndex = ServerVar.SLog.commitIndex

	envVar.Message = msg
	MsgAckMap[les.Lsn()] = 1

	//TODO send command on outchan->done

	response := make(chan bool)
	temp := &CommandTuple{Com: command.Bytes(), ComResponse: response}

	//fmt.Println("Broadcasted... Lsn = ",les.Logsn,"connection = ",conn)
	ServerVar.Outchan <- temp

	//ServerVar.Outbox() <- &envVar

	//UpdateGlobLogEntMap(les, conn)

	//fmt.Println("Updating map ",les.Logsn," --- ",conn)
	//fmt.Println("Append() lsn = ", les.Logsn)

	var msgvar ConMsg
	msgvar.Les = les
	msgvar.Con = conn
	
	
	select {
	case t := <-response:
		if t {
			
			CommitCh <- msgvar
			//fmt.Println("Received response for... LSN = ",les.Logsn,"connection = ",conn)
			//delete(MsgAckMap, les.Lsn())

		} else {

			returnmsg := "ERR_INTERNAL\r\n"
			conn.Write([]byte(returnmsg))

		}
	}

}

//Stores where to send reply for perticular log entry
func UpdateGlobLogEntMap(les LogEntry, conn net.Conn) {

	MutexLog.Lock()
	_, ok := LogEntMap[les.Lsn()]
	if !ok {
		LogEntMap[les.Lsn()] = conn
	}

	fmt.Println("UpdateGlobLogEntMap updated", les.Lsn(),"---",LogEntMap[les.Lsn()])
	MutexLog.Unlock()

}

func (ServerVar *Raft) parseRest(cmdtype int, res []string, conn net.Conn, reader *bufio.Reader) {

	//fmt.Println("Server ID ", ServerVar.ServId(), "parseRest()")

	if len(res) != 2 {
		returnmsg := "ERRCMDERR\r\n"
		conn.Write([]byte(returnmsg))
	} else {
		commandData := Command{cmdtype, res[1], -1, -1, ([]byte("")), -1}
		command := new(bytes.Buffer)
		encCommand := gob.NewEncoder(command)
		encCommand.Encode(commandData)

		//MutexAppend.Lock()
		ServerVar.Inprocess = true
		ServerVar.AppendUtility(conn, command, commandData)

	}
}

func (ServerVar *Raft) ConnHandler(conn net.Conn) {

	reader := bufio.NewReader(conn)

	for true {

		//......Reading command from the clients........

		cmd, err := reader.ReadBytes('\n')
		//fmt.Println("Server ID ", ServerVar.ServId(), "Reading commands connhandler()")
		var returnmsg string
		if err == io.EOF {
			returnmsg = "ERR_INTERNAL\r\n"
			io.Copy(conn, bytes.NewBufferString(returnmsg))
			break
		}

		if err != nil {
			returnmsg = "ERR_INTERNAL\r\n"
			io.Copy(conn, bytes.NewBufferString(returnmsg))
			break
		}
		command := string(cmd)
		command = strings.TrimSpace(command)

		res := strings.Split((command), " ")

		//.....Handling different types of commands possible.....
		switch res[0] {
		case "set":
			ServerVar.parseSetCas(1, res, conn, reader)
			break
		case "cas":
			ServerVar.parseSetCas(2, res, conn, reader)
			break
		case "get":
			ServerVar.parseRest(3, res, conn, reader)
			break
		case "getm":
			ServerVar.parseRest(4, res, conn, reader)
			break
		case "delete":
			ServerVar.parseRest(5, res, conn, reader)
			break

		default:
			returnmsg = "ERRCMDERR\r\n"
			conn.Write([]byte(returnmsg))
			break
		}

	}

	conn.Close()

}
