package raft

import (
	"bufio"
	"bytes"
	"encoding/gob"
	//"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

//var AtomicAppend chan int
var MutexAppend = &sync.RWMutex{}

//ClientServerComm() is used to listen at a port through whichs clients are communication to servers.

type Keyvalstore interface {
	parseSetCas(cmdtype int, res []string, conn net.Conn)

	parseRest(cmdtype int, res []string, conn net.Conn)
	AppendUtility(conn net.Conn, command *bytes.Buffer)

	ConnHandler(conn net.Conn)
}

func (ServerVar *Raft) ClientServerComm(clientPortAddr string) {

	//fmt.Println("Client Port : ", clientPortAddr)
	lis, error := net.Listen("tcp", clientPortAddr)
	defer lis.Close()

	if error != nil {
		panic("Client-Server connection error : " + error.Error())
	}

	for {

		con, error := lis.Accept()
		if error != nil {
			panic("Client-Server accept error : " + error.Error())
			continue
		}
		go ServerVar.ConnHandler(con)
		go KvReadCommitCh()
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

		MutexAppend.Lock()
		ServerVar.AppendUtility(conn, command)

	}

}

//AppendUtility() calls append() periodically after every 3 seconds for infinite time until consensus is reached among servers.
// Once concenus is arrived,it will return.
func (ServerVar *Raft) AppendUtility(conn net.Conn, command *bytes.Buffer) {

	ticker := time.NewTicker(3 * time.Second)

	go func() {
		for {
			les, er := ServerVar.Append(command.Bytes())

			if er == nil {
				UpdateGlobLogEntMap(les, conn)
			} else {
				returnmsg := er.Error()

				conn.Write([]byte(returnmsg))
				return

			}
			select {
			case <-ServerVar.GotConsensus:

				ticker.Stop()
				return
			}
			<-ticker.C

		}
	}()

}

func UpdateGlobLogEntMap(les LogEntry, conn net.Conn) {

	MutexLog.Lock()
	_, ok := LogEntMap[les.Lsn()]
	if !ok {
		LogEntMap[les.Lsn()] = conn
	}

	MutexLog.Unlock()

}

func (ServerVar *Raft) parseRest(cmdtype int, res []string, conn net.Conn, reader *bufio.Reader) {

	if len(res) != 2 {
		returnmsg := "ERRCMDERR\r\n"
		conn.Write([]byte(returnmsg))
	} else {
		commandData := Command{cmdtype, res[1], -1, -1, ([]byte("")), -1}
		command := new(bytes.Buffer)
		encCommand := gob.NewEncoder(command)
		encCommand.Encode(commandData)

		MutexAppend.Lock()
		ServerVar.AppendUtility(conn, command)

	}
}

func (ServerVar *Raft) ConnHandler(conn net.Conn) {

	reader := bufio.NewReader(conn)

	for true {

		//......Reading command from the clients........

		cmd, err := reader.ReadBytes('\n')

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
