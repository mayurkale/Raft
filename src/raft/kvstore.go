package raft

import (
	"bytes"
	"container/heap"
	"encoding/gob"
	"fmt"
	//"net"
	"strconv"
	//"sync"
	"time"
)

func (PQ PriorityQueue) Len() int { return len(PQ) }

//.........Min Priority Queue...........
func (PQ PriorityQueue) Less(i, j int) bool {
	return PQ[i].priority < PQ[j].priority
}

func (PQ PriorityQueue) Swap(i, j int) {
	PQ[i], PQ[j] = PQ[j], PQ[i]
	PQ[i].index = i
	PQ[j].index = j
}

func (PQ *PriorityQueue) Push(x interface{}) {
	n := len(*PQ)
	item := x.(*Item)
	item.index = n
	*PQ = append(*PQ, item)
	heap.Fix(PQ, item.index)
}

func (PQ *PriorityQueue) Pop() interface{} {
	old := *PQ
	n := len(old)
	item := old[n-1]
	item.index = -1
	*PQ = old[0 : n-1]
	return item
}

//Fuction to to apply old entries to state machine
func (ServerVar *Raft) ApplyCommandToSM() {
	for {
		//fmt.Println("State machine Apply entered Server ID : ", ServerVar.ServId())
		les := <-ServerVar.Inchan
		//fmt.Println("State machine Apply listened Server ID : ", ServerVar.ServId())

		var decoddata Command
		cmddcd := bytes.NewBuffer(les.DataArray)
		cmd := gob.NewDecoder(cmddcd)
		cmd.Decode(&decoddata)

if debug {
		fmt.Println("Server ID : ", ServerVar.ServId(), "  Applying command : ", decoddata.CmdType, "Key = ", decoddata.Key)
}
		switch decoddata.CmdType {

		case 1:
			_ = SetCmdReturn(decoddata)
			break
		case 2:
			_ = CasCmdReturn(decoddata)
			break
		/*case 3:
			_ = GetCmdReturn(decoddata)
			break
		case 4:
			_ = GetMCmdReturn(decoddata)
			break*/
		case 5:
			_ = DeleteCmdReturn(decoddata)
			break

		default:
			_ = "ERRCMDERR\r\n"
			break
		}
	}

}

//Fuction to read command from CommitCh channel and process
func KvReadCommitCh() {

	for {
		les := <-CommitCh

		var decoddata1 Command
		cmddcd1 := bytes.NewBuffer(les.DataArray)
		cmd1 := gob.NewDecoder(cmddcd1)
		cmd1.Decode(&decoddata1)

//		fmt.Println("Got for commitch ", decoddata1.CmdType, " key = ", decoddata1.Key, "LSN = ", les.Logsn)

		var decoddata Command
		cmddcd := bytes.NewBuffer(les.DataArray)
		cmd := gob.NewDecoder(cmddcd)
		cmd.Decode(&decoddata)

		var ret string

		switch decoddata.CmdType {
		case 1:
			ret = SetCmdReturn(decoddata)
			break
		case 2:
			ret = CasCmdReturn(decoddata)
			break
		case 3:
			ret = GetCmdReturn(decoddata)

			break
		case 4:
			ret = GetMCmdReturn(decoddata)
			break
		case 5:
			ret = DeleteCmdReturn(decoddata)
			break

		default:
			ret = "ERRCMDERR\r\n"
			break
		}

		//Lock and unlock Logentry-client Map
		MutexLog.RLock()
		conn, _ := LogEntMap[les.Logsn] // conn is connection object to respond back to respective client
		MutexLog.RUnlock()

		//Response to client based on type of statement
		conn.Write([]byte(ret))

	}

}

func SetCmdReturn(CommandData Command) string {

	version := int64(0)

	//Acquire xclusive lock
	mutex.Lock()
	if _, key_exist := keyval[CommandData.Key]; key_exist {

		version = keyval[CommandData.Key].version + 1
	}

	curr_time := time.Now().Unix()

	keyval[CommandData.Key] = valstruct{version, CommandData.Expirytime, curr_time, CommandData.Len, CommandData.Value}

	//If expiry time == 0, no need to add to priority queue as that entry never have to be deleted
	if CommandData.Expirytime != 0 {
		item := &Item{
			value:     CommandData.Key,
			priority:  curr_time + int64(CommandData.Expirytime),
			timestamp: curr_time,
		}
		heap.Push(&PQ, item)
	}
	mutex.Unlock()
	//Release xclusive lock

	returnmsg := "OK " + strconv.FormatInt(version, 10) + "\r\n"
	return returnmsg
}

func CasCmdReturn(CommandData Command) string {

	var returnmsg string
	//Acquire xclusive lock
	mutex.Lock()
	if _, key_exist := keyval[CommandData.Key]; !key_exist {

		mutex.Unlock()
		//Release xclusive lock
		returnmsg = "ERRNOTFOUND\r\n"

	} else if keyval[CommandData.Key].version != CommandData.Version {

		mutex.Unlock()
		//Release xclusive lock
		returnmsg = "ERR_VERSION\r\n"

	} else {

		version := keyval[CommandData.Key].version + 1
		curr_time := time.Now().Unix()

		keyval[CommandData.Key] = valstruct{version, CommandData.Expirytime, curr_time, CommandData.Len, CommandData.Value}
		//....If expiry time == 0, no need to add to priority queue as that entry never have to be deleted
		if CommandData.Expirytime != 0 {
			item := &Item{
				value:     CommandData.Key,
				priority:  curr_time + int64(CommandData.Expirytime),
				timestamp: curr_time,
			}
			heap.Push(&PQ, item)
		}
		mutex.Unlock()
		//Release xclusive lock
		returnmsg = "OK " + strconv.FormatInt(version, 10) + "\r\n"
	}
	return returnmsg

}

func GetCmdReturn(CommandData Command) string {

	//Acquire read lock
	mutex.RLock()
	valprint, val_exist := keyval[CommandData.Key]
	mutex.RUnlock()
	//Release read lock
	var returnmsg string
	if val_exist && (int64(valprint.expirytime)+valprint.timestamp) >= time.Now().Unix() {

		returnmsg = "VALUE " + strconv.Itoa(valprint.numbytes) + "\r\n" + string(valprint.value) + "\r\n"

	} else {

		returnmsg = "ERRNOTFOUND\r\n"
	}

	return returnmsg
}

func GetMCmdReturn(CommandData Command) string {

	//Acquire read lock
	mutex.RLock()
	valprint, val_exist := keyval[CommandData.Key]
	mutex.RUnlock()
	//Release read lock
	var returnmsg string
	if val_exist && (int64(valprint.expirytime)+valprint.timestamp) >= time.Now().Unix() {

		expiraytime_left := int64(valprint.expirytime) - (time.Now().Unix() - valprint.timestamp)
		returnmsg = "VALUE" + " " + strconv.FormatInt(valprint.version, 10) + " " + strconv.FormatInt(expiraytime_left, 10) + " " + strconv.Itoa(valprint.numbytes) + "\r\n" + string(valprint.value) + "\r\n"

	} else {

		returnmsg = "ERRNOTFOUND\r\n"
	}
	return returnmsg
}

func DeleteCmdReturn(CommandData Command) string {

	var returnmsg string
	//Acquire read lock
	mutex.Lock()
	if _, val_exist := keyval[CommandData.Key]; val_exist {
		delete(keyval, CommandData.Key)
		mutex.Unlock()
		//Release xclusive lock
		returnmsg = "DELETED\r\n"
	} else {
		mutex.Unlock()
		//Release xclusive lock
		returnmsg = "ERRNOTFOUND\r\n"
	}
	return returnmsg
}

func Clear_expired_keys() {

	timer := time.NewTicker(time.Millisecond * 4000)
	go func() {
		for range timer.C {
			//Acquire xclusive lock
			mutex.Lock()

			for PQ.Len() > 0 && PQ[0].priority < time.Now().Unix() {

				top_element := PQ[0]
				if val, key_exist := keyval[top_element.value]; key_exist == true && top_element.timestamp == val.timestamp {
					delete(keyval, top_element.value)
				} else {
					//........no-operation required...........
				}
				top_element = heap.Pop(&PQ).(*Item)

			}

			mutex.Unlock()
			//Release xclusive lock
		}
	}()

}
