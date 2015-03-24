package main

import (
	"container/heap"
	"flag"
	//"fmt"
	"net"
	"raft"
	"sync"
)

func main() {

	numbPtr := flag.Int("id", -1, "an int")
	flag.Parse()
	if *numbPtr < 0 {
		panic("Server ID cannot be negative : Set a positive valued Id flag")
	}

	var wg *sync.WaitGroup
	wg = new(sync.WaitGroup)

	raft.MsgAckMap = make(map[raft.Lsn]int)
	raft.LogEntMap = make(map[raft.Lsn]net.Conn)
	raft.CommitCh = make(chan raft.LogEntryStruct)

	// heap implementation to delete expired keys
	heap.Init(&(raft.PQ))
	go raft.Clear_expired_keys()

	//FireAServer() will in turn call connhandler() which calls append()
	//Append() immediately returns after initiating disk write and broadcast to other replica without waiting for results

	serverVar := raft.FireAServer(*numbPtr)

	//Start() starts the raft server operations in follower mode
	serverVar.Start()

	wg.Add(1)
	wg.Wait()
}
