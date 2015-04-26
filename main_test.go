package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	//"math/rand"
	"net"
	"os"
	"os/exec"
	"raft"
	"strconv"
	"strings"
	//"sync"
	"testing"
	"time"
)

var noOfThreads int = 3
var noOfRequestsPerThread int = 10

//var wgroup sync.WaitGroup
var commands []string
var procmap map[int]*exec.Cmd
var servermap map[int]bool

func init() {

	go launch_servers()
	time.Sleep(time.Second * 10)

}

var no_servers int

func launch_servers() {
	fileName := "clusterConfig.json"
	var obj raft.ClusterConfig
	file, e := ioutil.ReadFile(fileName)

	if e != nil {
		panic("File error: " + e.Error())
	}

	json.Unmarshal(file, &obj)

	no_servers, _ = strconv.Atoi(obj.Count.Count)
	procmap = make(map[int]*exec.Cmd)
	servermap = make(map[int]bool)
	for i := 1; i <= no_servers; i++ {
		//wgroup.Add(1)
		go execute_cmd(i)
	}

	//wgroup.Wait()

}

func execute_cmd(id int) {

	//fmt.Println("Server Id = ", id)

	cmd := exec.Command("./bin/kvstore", "-id="+strconv.Itoa(id))

	servermap[id] = true
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	//fmt.Println("SErvermap = ", servermap)
	procmap[id] = cmd
	err := cmd.Run()
	if err != nil {
		//wgroup.Done()
	}

	cmd.Wait()
	//wgroup.Done()

}

/*
func TestLeaderKill(t *testing.T) {

	slice := []int{3, 4, 5}
        check := false
        for _,elem := range slice{
	if err := procmap[elem].Process.Kill(); err != nil {
            t.Log("failed to kill: ", err)
        }
 	t.Log("YOOYOYOYOYOMMMMMMMM")
 	check = true
  	wgroup.Done()
        }

        //wgroup.Add(1)
	go execute_cmd(3)
	if err := procmap[3].Process.Kill(); err != nil {
            t.Log("failed to kill: ", err)

        } else {
          check = true
        }

	if !check {
		t.Error("TestLeaderElect1 Failed, please check implementation")
	} else {

		fmt.Println("Testcase - TestLeaderElect1 : Passed")
	}
}

*/

//-------------------------------------------------------------------------------------------------------------------
// Helper function to get the ServerId of current leader, using ping to a random server
//------------------------------------------------------------------------------------------------------------------
func get_LeaderId(pingServId string) string {
	//fmt.Println("in leader  ")
	ServAdd := "127.0.0.1:900" + pingServId
	conn, _ := net.Dial("tcp", ServAdd)
	//fmt.Println("conn established  ")
	//time.Sleep(5 * time.Second)
	//fmt.Println("before re  ")
	reader := bufio.NewReader(conn)
	//fmt.Println("after re  ")
	leaderId := "-1"
	for leaderId == "-1" {
		//fmt.Println("in loop  ")
		io.Copy(conn, bytes.NewBufferString("get leaderId\r\n"))

		resp, _ := reader.ReadBytes('\n')
		//fmt.Println("resd bytes  ")
		response := strings.Split(strings.TrimSpace(string(resp)), " ")
		//fmt.Println(string(resp))

		if response[0] == "ERRNOTFOUND" {
			leaderId = pingServId
		} else if response[0] == "Redirect" && response[3] != "-1" {
			id, _ := strconv.Atoi(response[3])
			if _, ok := servermap[id]; ok {
				leaderId = response[3]
			}
		}
	}
	conn.Close()
	return leaderId
}

func returnRandServ() int {

	for k, v := range servermap {
		if v == true {
			return k
		}
	}
	return -1
}

func killServer(id int) {

	if err1 := procmap[id].Process.Kill(); err1 != nil {
		fmt.Println("failed to kill: ", err1)
	} else {
		delete(servermap, id)
	}
}

//..............................................................................................................................//

// Testcase to test Leader Election - 1 (First Leader Elect)

//..............................................................................................................................//
func TestLeaderElect1(t *testing.T) {

	tester := make(chan bool)
	go check_LeaderElect1(t, tester)
	check := <-tester

	if !check {
		t.Error("TestLeaderElect1 Failed, please check implementation")
	} else {

		fmt.Println("Testcase - TestLeaderElect1 : Passed")
	}
}

func check_LeaderElect1(t *testing.T, tester chan bool) {

	LeaderAdd := "127.0.0.1:900" + get_LeaderId("1")
	conn, error := net.Dial("tcp", LeaderAdd)
	if error != nil {
		tester <- false
		conn.Close()
	}
	io.Copy(conn, bytes.NewBufferString("get LeaderTester\r\n"))
	reader := bufio.NewReader(conn)
	resp, error := reader.ReadBytes('\n')
	if error != nil {
		conn.Close()
	} else {
		response := strings.Split(strings.TrimSpace(string(resp)), " ")
		if response[0] == "ERRNOTFOUND" {
			tester <- true
		} else {
			tester <- false
		}

	}
	conn.Close()
}

//..............................................................................................................................//

// Testcase to test Basic Set and Get

//..............................................................................................................................//

func TestSetnGet(t *testing.T) {

	tester := make(chan bool)
	go check_SetnGet(t, tester)
	check := <-tester

	if !check {
		t.Error("SetnGet Failed, please check implementation")
	} else {

		fmt.Println("Testcase - TestSetnGet : Passed")
	}
}

func check_SetnGet(t *testing.T, tester chan bool) {

	LeaderAdd := "127.0.0.1:900" + get_LeaderId("1")
	conn, error := net.Dial("tcp", LeaderAdd)
	if error != nil {
		tester <- false
		conn.Close()
	}

	io.Copy(conn, bytes.NewBufferString("set Roll101 50 5\r\nTarun\r\n"))
	reader := bufio.NewReader(conn)
	resp, error := reader.ReadBytes('\n')
	if error != nil {
		conn.Close()
	} else {
		if len(string(resp)) > 0 {
			response := strings.Split(strings.TrimSpace(string(resp)), " ")
			if response[0] != "OK" {
				tester <- false
				conn.Close()
			}

		} else {
			tester <- false
			conn.Close()
		}

	}

	io.Copy(conn, bytes.NewBufferString("get Roll101\r\n"))
	resp, error = reader.ReadBytes('\n')
	if error != nil {
		conn.Close()
	} else {
		response := strings.Split(strings.TrimSpace(string(resp)), " ")
		if response[0] == "VALUE" || response[1] == "5" {

			res, error := reader.ReadString('\n')
			if error != nil {
				conn.Close()
			} else {

				if res == "Tarun\r\n" {
					tester <- true
					conn.Close()
				} else {
					tester <- false
					conn.Close()
				}

			}

		} else {
			tester <- false
			conn.Close()
		}

	}
	conn.Close()
}

//..............................................................................................................................//

// Testcase to test basic Set and GetM operation

//..............................................................................................................................//
func TestSetnGETM(t *testing.T) {

	tester := make(chan bool)
	go check_SetnGetM(t, tester)
	check := <-tester

	if !check {
		t.Error("SetnGetM Failed, please check implementation")
	} else {
		fmt.Println("Testcase - TestSetnGetM : Passed")
	}
}

func check_SetnGetM(t *testing.T, tester chan bool) {

	LeaderAdd := "127.0.0.1:900" + get_LeaderId("1")
	conn, error := net.Dial("tcp", LeaderAdd)
	if error != nil {
		tester <- false
		conn.Close()
	}
	reader := bufio.NewReader(conn)
	io.Copy(conn, bytes.NewBufferString("set Roll102 50 9\r\nTarunjain\r\n"))
	resp, err := reader.ReadBytes('\n')
	io.Copy(conn, bytes.NewBufferString("set Roll102 500 9\r\njainTarun\r\n"))
	resp, err = reader.ReadBytes('\n')
	io.Copy(conn, bytes.NewBufferString("getm Roll102\r\n"))
	resp, err = reader.ReadBytes('\n')

	res := string(resp)
	//fmt.Println(res)
	if err != nil {
		tester <- false
		conn.Close()
	} else {
		res = strings.TrimRight(string(res), "\r\n")
		//fmt.Println(res)
		response := strings.Split(strings.TrimSpace(res), " ")
		if len(response) == 4 && response[0] == "VALUE" {
			res, err = reader.ReadString('\n')
			if err != nil && res != "jainTarun\r\n" {
				tester <- false
				conn.Close()
			} else {
				tester <- true
				conn.Close()
			}
		} else {
			tester <- true
			conn.Close()
		}
	}

	conn.Close()
}

//..............................................................................................................................//

// Testcase: To test error conditions

//..............................................................................................................................//

func TestCheckError(t *testing.T) {

	tester := make(chan bool)
	go check_error(t, tester)
	check := <-tester

	if !check {
		t.Error("TestCheckError Failed, please check implementation")
	} else {
		fmt.Println("Testcase - TestCheckError : Passed")
	}
}

func check_error(t *testing.T, tester chan bool) {

	LeaderAdd := "127.0.0.1:900" + get_LeaderId("1")
	conn, error := net.Dial("tcp", LeaderAdd)
	if error != nil {
		tester <- false
		conn.Close()
	}
	reader := bufio.NewReader(conn)

	io.Copy(conn, bytes.NewBufferString("set\r\n"))
	res, err := reader.ReadBytes('\n')
	if err != nil || string(res) != "ERRCMDERR\r\n" {
		tester <- false
		conn.Close()
	}

	io.Copy(conn, bytes.NewBufferString("set key\r\n"))
	res, err = reader.ReadBytes('\n')
	if err != nil || string(res) != "ERRCMDERR\r\n" {
		tester <- false
		conn.Close()
	}

	io.Copy(conn, bytes.NewBufferString("set key abc 4\r\n"))
	res, err = reader.ReadBytes('\n')
	if err != nil || string(res) != "ERRCMDERR\r\n" {
		tester <- false
		conn.Close()
	}

	io.Copy(conn, bytes.NewBufferString("set keysgbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 10 4\r\n"))
	res, err = reader.ReadBytes('\n')
	if err != nil || string(res) != "ERRCMDERR\r\n" {
		tester <- false
		conn.Close()
	}

	io.Copy(conn, bytes.NewBufferString("set key 10 4 noreply\r\n"))
	res, err = reader.ReadBytes('\n')
	if err != nil || string(res) != "ERRCMDERR\r\n" {
		tester <- false
		conn.Close()
	}

	io.Copy(conn, bytes.NewBufferString("set key 10 4\r\n"))
	io.Copy(conn, bytes.NewBufferString("abcdef\r\n"))
	res, err = reader.ReadBytes('\n')
	if err != nil || string(res) != "ERRCMDERR\r\n" {
		tester <- false
		conn.Close()
	}

	io.Copy(conn, bytes.NewBufferString("get\r\n"))
	res, err = reader.ReadBytes('\n')
	if err != nil || string(res) != "ERRCMDERR\r\n" {
		tester <- false
		conn.Close()
	}

	io.Copy(conn, bytes.NewBufferString("get 101\r\n"))
	res, err = reader.ReadBytes('\n')
	if err != nil || string(res) != "ERRNOTFOUND\r\n" {
		tester <- false
		conn.Close()
	}

	io.Copy(conn, bytes.NewBufferString("getm\r\n"))
	res, err = reader.ReadBytes('\n')
	if err != nil || string(res) != "ERRCMDERR\r\n" {
		tester <- false
		conn.Close()
	}

	io.Copy(conn, bytes.NewBufferString("getm 101\r\n"))
	res, err = reader.ReadBytes('\n')
	if err != nil || string(res) != "ERRNOTFOUND\r\n" {
		tester <- false
		conn.Close()
	}

	io.Copy(conn, bytes.NewBufferString("cas\r\n"))
	res, err = reader.ReadBytes('\n')
	if err != nil || string(res) != "ERRCMDERR\r\n" {
		tester <- false
		conn.Close()
	}

	io.Copy(conn, bytes.NewBufferString("set ROLL1 50 2\r\ntj\r\n"))
	res, err = reader.ReadBytes('\n')
	io.Copy(conn, bytes.NewBufferString("cas ROLL1 500 1 5\r\nabcde\r\n"))
	res, err = reader.ReadBytes('\n')
	if err != nil || string(res) != "ERR_VERSION\r\n" {
		tester <- false
		conn.Close()
	}

	io.Copy(conn, bytes.NewBufferString("cas ROLL2 500 1 5\r\nabcde\r\n"))
	res, err = reader.ReadBytes('\n')
	if err != nil || string(res) != "ERRNOTFOUND\r\n" {
		tester <- false
		conn.Close()
	}

	io.Copy(conn, bytes.NewBufferString("delete ROLL2\r\n"))
	res, err = reader.ReadBytes('\n')
	if err != nil || string(res) != "ERRNOTFOUND\r\n" {
		tester <- false
		conn.Close()
	}
	tester <- true
	conn.Close()
}

//..............................................................................................................................//

// Testcase: To test error condition of Error-Redirect and all servers redirect to same server  : Leader

//..............................................................................................................................//

func TestCheckErrorRedirect(t *testing.T) {

	tester := make(chan bool)
	go check_errorRedirect(t, tester)
	check := <-tester

	if !check {
		t.Error("TestCheckErrorRedirect Failed, please check implementation")
	} else {
		fmt.Println("Testcase - TestCheckErrorRedirect : Passed")
	}
}

func check_errorRedirect(t *testing.T, tester chan bool) {

	LeaderId := get_LeaderId("1")

	slice := []string{"2", "3", "4", "5"}

	for _, elem := range slice {

		if LeaderId != get_LeaderId(elem) {
			tester <- false
			break
		} else {
			tester <- true
		}

	}

}

//..............................................................................................................................//

//Testcase to test basic Set and Expiray

//..............................................................................................................................//

func TestSetnExp(t *testing.T) {

	tester := make(chan bool)
	go check_SetnExp(t, tester)
	check := <-tester

	if !check {
		t.Error("SetnExpiray Failed, please check implementation")
	} else {
		fmt.Println("Testcase - TestSetnExpiary : Passed")
	}
}

func check_SetnExp(t *testing.T, tester chan bool) {

	LeaderAdd := "127.0.0.1:900" + get_LeaderId("1")
	conn, error := net.Dial("tcp", LeaderAdd)
	if error != nil {
		tester <- false
		conn.Close()
	}

	reader := bufio.NewReader(conn)

	io.Copy(conn, bytes.NewBufferString("set Roll104 5 3\r\nTj1\r\n"))
	res, err := reader.ReadBytes('\n')
	io.Copy(conn, bytes.NewBufferString("set Roll105 10 3\r\nTj2\r\n"))
	res, err = reader.ReadBytes('\n')
	io.Copy(conn, bytes.NewBufferString("set Roll106 15 3\r\nTj3\r\n"))
	res, err = reader.ReadBytes('\n')
	time.Sleep(time.Second * 6)

	io.Copy(conn, bytes.NewBufferString("getm Roll104\r\n"))
	res, err = reader.ReadBytes('\n')

	if err == nil && string(res) == "ERRNOTFOUND\r\n" {
		io.Copy(conn, bytes.NewBufferString("getm Roll105\r\n"))
		res, err = reader.ReadBytes('\n')
		if err != nil {
			tester <- false
			conn.Close()
		} else {
			resp := strings.TrimRight(string(res), "\r\n")
			//fmt.Println(res)
			response := strings.Split(strings.TrimSpace(resp), " ")
			if len(response) == 4 && response[0] == "VALUE" {
				res, err = reader.ReadBytes('\n')
				if err != nil && string(res) != "Tj2\r\n" {
					tester <- false
					conn.Close()
				} else {
					time.Sleep(time.Second * 11)
					io.Copy(conn, bytes.NewBufferString("getm Roll106\r\n"))
					res1, err1 := reader.ReadBytes('\n')
					io.Copy(conn, bytes.NewBufferString("delete Roll105\r\n"))
					res2, err2 := reader.ReadBytes('\n')

					if err1 == nil && err2 == nil && string(res1) == "ERRNOTFOUND\r\n" && string(res2) == "ERRNOTFOUND\r\n" {
						tester <- true
						conn.Close()
					} else {
						tester <- false
						conn.Close()
					}
				}
			} else {
				tester <- false
				conn.Close()
			}
		}

	} else {

		tester <- false
		conn.Close()

	}

	conn.Close()

}

//..............................................................................................................................//

//Testcase to test basic Set and Cas operation

//..............................................................................................................................//

func TestSetnCas(t *testing.T) {

	tester := make(chan bool)
	go check_SetnCas(t, tester)
	check := <-tester

	if !check {
		t.Error("SetnCas Failed, please check implementation")
	} else {
		fmt.Println("Testcase - TestSetnCas : Passed")
	}
}

func check_SetnCas(t *testing.T, tester chan bool) {

	LeaderAdd := "127.0.0.1:900" + get_LeaderId("1")
	conn, error := net.Dial("tcp", LeaderAdd)
	if error != nil {
		tester <- false
		conn.Close()
	}
	reader := bufio.NewReader(conn)

	io.Copy(conn, bytes.NewBufferString("set Roll103 50 9\r\nTarunjain\r\n"))
	res, err := reader.ReadBytes('\n')
	io.Copy(conn, bytes.NewBufferString("cas Roll103 500 0 5\r\nAkash\r\n"))
	res, err = reader.ReadBytes('\n')
	io.Copy(conn, bytes.NewBufferString("getm Roll103\r\n"))

	res, err = reader.ReadBytes('\n')

	if err != nil {
		tester <- false
		conn.Close()
	} else {
		resp := strings.TrimRight(string(res), "\r\n")
		response := strings.Split(strings.TrimSpace(resp), " ")
		if len(response) == 4 && response[0] == "VALUE" && response[1] == "1" {
			res, err = reader.ReadBytes('\n')
			if err != nil && string(res) != "Akash\r\n" {
				tester <- false
				conn.Close()
			} else {

				io.Copy(conn, bytes.NewBufferString("cas Roll103 500 1 3\r\nAbi\r\n"))
				res, err := reader.ReadBytes('\n')

				if err != nil {
					tester <- false
					conn.Close()
				} else {
					if string(res) == "OK 2\r\n" {
						tester <- true
						conn.Close()
					} else {
						tester <- false
						conn.Close()
					}
				}

			}
		} else {
			tester <- false
			conn.Close()
		}
	}

	conn.Close()
}

//..............................................................................................................................//

//Testcase to test basic Set and Delete operation

//..............................................................................................................................//

func TestSetnDel(t *testing.T) {

	tester := make(chan bool)
	go check_SetnDel(t, tester)
	check := <-tester

	if !check {
		t.Error("SetnDelete Failed, please check implementation")
	} else {
		fmt.Println("Testcase - TestSetnDelete : Passed")
	}
}

func check_SetnDel(t *testing.T, tester chan bool) {

	LeaderAdd := "127.0.0.1:900" + get_LeaderId("1")
	conn, error := net.Dial("tcp", LeaderAdd)
	if error != nil {
		tester <- false
		conn.Close()
	}
	reader := bufio.NewReader(conn)
	//------------Delete Check----------------------
	io.Copy(conn, bytes.NewBufferString("set Roll104 50 9\r\nTarunjain\r\n"))
	res, err := reader.ReadString('\n')

	io.Copy(conn, bytes.NewBufferString("delete Roll104\r\n"))
	res, err = reader.ReadString('\n')
	if err != nil {
		tester <- false
		conn.Close()
	} else {
		if string(res) == "DELETED\r\n" {
			tester <- true
			conn.Close()
		} else {
			tester <- false
			conn.Close()
		}
	}

	conn.Close()
}

/*
//..............................................................................................................................//

// Testcase: To test whether multiple clients works together properly.

//..............................................................................................................................//

func TestMulticlient(t *testing.T) {

	tester := make(chan int)
        Serv := returnRandServ()
        LeaderIdReturned := get_LeaderId(strconv.Itoa(Serv))
	for i := 0; i < noOfThreads; i++ {
		go multiclient(t, tester, LeaderIdReturned)
	}

	count := 0
	for i := 0; i < noOfThreads; i++ {
		count += <-tester
	}
	if count != noOfThreads {
		t.Error("TestMulticlient Failed")
	} else {
		fmt.Println("Testcase - TestMulticlient : Passed")
	}

}

func multiclient(t *testing.T, tester chan int, LeaderIdReturned string) {

        //Serv := returnRandServ()
	//LeaderIdReturned := get_LeaderId(strconv.Itoa(Serv))
	LeaderAdd := "127.0.0.1:900" + LeaderIdReturned
	conn, error := net.Dial("tcp", LeaderAdd)
	if error != nil {
		conn.Close()
	}
	reader := bufio.NewReader(conn)
	io.Copy(conn, bytes.NewBufferString("set key71 500 2\r\ntj\r\n"))
	res, err := reader.ReadBytes('\n')
	if err != nil {
		conn.Close()
	} else {
		if strings.Contains(strings.TrimSpace(string(res)), "OK") {
			tester <- 1
		} else {
			tester <- 0
		}

	}
	conn.Close()
}

*/

//..............................................................................................................................//

// Testcase to test Basic Set and Get (checking log replication, using server killing)

//..............................................................................................................................//

func TestSetnGetKill(t *testing.T) {

	tester := make(chan bool)
	go check_SetnGetKill(t, tester)
	check := <-tester

	if !check {
		t.Error("SetnGetKill Failed, please check implementation")
	} else {

		fmt.Println("Testcase - TestSetnGetKill : Passed")
	}
}

func check_SetnGetKill(t *testing.T, tester chan bool) {

	Serv := returnRandServ()

	LeaderIdReturned := get_LeaderId(strconv.Itoa(Serv))
	//fmt.Println("hi ",LeaderIdReturned)
	LeaderId, _ := strconv.Atoi(LeaderIdReturned)
	LeaderAdd := "127.0.0.1:900" + LeaderIdReturned
	conn, error := net.Dial("tcp", LeaderAdd)
	if error != nil {
		tester <- false
		conn.Close()
	}

	io.Copy(conn, bytes.NewBufferString("set key51 50 5\r\nTarun\r\n"))
	reader := bufio.NewReader(conn)
	resp, error := reader.ReadBytes('\n')
	if error != nil {
		conn.Close()
	} else {
		if len(string(resp)) > 0 {
			response := strings.Split(strings.TrimSpace(string(resp)), " ")
			if response[0] != "OK" {
				tester <- false
				conn.Close()
			}

		} else {
			tester <- false
			conn.Close()
		}

	}

	killServer(LeaderId)

	Serv1 := returnRandServ()
	//fmt.Println(servermap)
	//fmt.Println(Serv1)

	LeaderIdReturned1 := get_LeaderId(strconv.Itoa(Serv1))
	//fmt.Println(LeaderIdReturned1)

	LeaderId1, _ := strconv.Atoi(LeaderIdReturned1)

	//fmt.Println(LeaderId1)

	killServer(LeaderId1)
	//time.Sleep(time.Second * 1)
	Serv2 := returnRandServ()
	//fmt.Println(servermap)
	//fmt.Println(Serv2)

	LeaderIdReturned2 := get_LeaderId(strconv.Itoa(Serv2))
	//fmt.Println(LeaderIdReturned2)
	//LeaderId2,_ := strconv.Atoi(LeaderIdReturned2)

	LeaderAdd = "127.0.0.1:900" + LeaderIdReturned2
	conn1, error := net.Dial("tcp", LeaderAdd)
	if error != nil {
		tester <- false
		conn1.Close()
	}

	io.Copy(conn1, bytes.NewBufferString("get key51\r\n"))
	reader = bufio.NewReader(conn1)
	resp, error = reader.ReadBytes('\n')
	if error != nil {
		conn1.Close()
	} else {
		response := strings.Split(strings.TrimSpace(string(resp)), " ")
		if response[0] == "VALUE" || response[1] == "5" {

			res, error := reader.ReadString('\n')
			if error != nil {
				conn1.Close()
			} else {

				if res == "Tarun\r\n" {
					tester <- true
					conn1.Close()
				} else {
					tester <- false
					conn1.Close()
				}

			}

		} else {
			tester <- false
			conn1.Close()
		}

	}

	//wgroup.Add(1)
	go execute_cmd(LeaderId) //restarting the killed servers

	//wgroup.Add(1)
	go execute_cmd(LeaderId1) //restarting the killed servers
	//time.Sleep(time.Second * 2)

	//tester <- true
	//wgroup.Wait()
	conn1.Close()
}

//..............................................................................................................................//

// Testcase to test Leader Election - 2 (Killing the Fisrt Leader And another random Server)

//..............................................................................................................................//

func TestLeaderElect2(t *testing.T) {

	tester := make(chan bool)
	time.Sleep(time.Second * 2)
	go check_LeaderElect2(t, tester)
	check := <-tester

	if !check {
		t.Error("TestLeaderElect2 Failed, please check implementation")
	} else {

		fmt.Println("Testcase - TestLeaderElect2 : Passed")
	}
}

func check_LeaderElect2(t *testing.T, tester chan bool) {

	Serv := returnRandServ()
	//fmt.Println("hi = ",Serv)
	LeaderIdReturned := get_LeaderId(strconv.Itoa(Serv))
	//fmt.Println("hi = ",LeaderIdReturned)

	LeaderId, _ := strconv.Atoi(LeaderIdReturned)
	killServer(LeaderId)

	Serv1 := returnRandServ()
	killServer(Serv1)
	//fmt.Println(Serv1)
	Serv2 := returnRandServ()
	//time.Sleep(time.Second * 2)
	//fmt.Println(Serv2)

	LeaderIdReturned = get_LeaderId(strconv.Itoa(Serv2))

	LeaderAdd := "127.0.0.1:900" + LeaderIdReturned
	//fmt.Println(LeaderAdd)

	conn, error := net.Dial("tcp", LeaderAdd)
	if error != nil {
		tester <- false
		conn.Close()
	}

	io.Copy(conn, bytes.NewBufferString("get LeaderTester\r\n"))
	reader := bufio.NewReader(conn)
	resp, error := reader.ReadBytes('\n')
	if error != nil {
		conn.Close()
	} else {
		response := strings.Split(strings.TrimSpace(string(resp)), " ")
		if response[0] == "ERRNOTFOUND" {
			tester <- true
			conn.Close()
		} else {
			tester <- false
			conn.Close()
		}

	}

	//wgroup.Add(1)
	go execute_cmd(LeaderId)
	//wgroup.Add(1)
	go execute_cmd(Serv1)
	//tester<-true

}

//..............................................................................................................................//

// Testcase to test Basic Set and Get (checking log replication, using server killing)

//..............................................................................................................................//

func TestSetnGetKillMajority(t *testing.T) {

	tester := make(chan bool)
	go check_SetnGetKillMajority(t, tester)
	check := <-tester

	if !check {
		t.Error("SetnGetKillMajority Failed, please check implementation")
	} else {

		fmt.Println("Testcase - TestSetnGetKillMajority : Passed")
	}
}

func check_SetnGetKillMajority(t *testing.T, tester chan bool) {

	Serv := returnRandServ()

	LeaderIdReturned := get_LeaderId(strconv.Itoa(Serv))
	//fmt.Println("hi ",LeaderIdReturned)
	LeaderId, _ := strconv.Atoi(LeaderIdReturned)
	LeaderAdd := "127.0.0.1:900" + LeaderIdReturned
	conn, error := net.Dial("tcp", LeaderAdd)
	if error != nil {
		tester <- false
		conn.Close()
	}

	io.Copy(conn, bytes.NewBufferString("set key51 50 5\r\nTarun\r\n"))
	reader := bufio.NewReader(conn)
	resp, error := reader.ReadBytes('\n')
	if error != nil {
		conn.Close()
	} else {
		if len(string(resp)) > 0 {
			response := strings.Split(strings.TrimSpace(string(resp)), " ")
			if response[0] != "OK" {
				tester <- false
				conn.Close()
			}

		} else {
			tester <- false
			conn.Close()
		}

	}

	killServer(LeaderId)

	Serv1 := returnRandServ()
	LeaderIdReturned1 := get_LeaderId(strconv.Itoa(Serv1))
	LeaderId1, _ := strconv.Atoi(LeaderIdReturned1)
	killServer(LeaderId1)

	Serv2 := returnRandServ()
	LeaderIdReturned2 := get_LeaderId(strconv.Itoa(Serv2))
	LeaderId2, _ := strconv.Atoi(LeaderIdReturned2)
	killServer(LeaderId2)

	//go restartServerAfterSomeTime(LeaderId2)

	Serv3 := returnRandServ()
	LeaderIdReturned3 := get_LeaderId(strconv.Itoa(Serv3))
	//LeaderId3,_ := strconv.Atoi(LeaderIdReturned2)
	//killServer(LeaderId2)

	LeaderAdd = "127.0.0.1:900" + LeaderIdReturned3
	conn1, error := net.Dial("tcp", LeaderAdd)
	if error != nil {
		tester <- false
		conn1.Close()
	}

	io.Copy(conn1, bytes.NewBufferString("get key51\r\n"))
	reader = bufio.NewReader(conn1)
	resp, error = reader.ReadBytes('\n')
	if error != nil {
		conn1.Close()
	} else {
		response := strings.Split(strings.TrimSpace(string(resp)), " ")
		if response[0] == "VALUE" || response[1] == "5" {

			res, error := reader.ReadString('\n')
			if error != nil {
				conn1.Close()
			} else {

				if res == "Tarun\r\n" {
					tester <- true
					conn1.Close()
				} else {
					tester <- false
					conn1.Close()
				}

			}

		} else {
			tester <- false
			conn1.Close()
		}

	}

	//wgroup.Add(1)
	go execute_cmd(LeaderId) //restarting the killed servers

	//wgroup.Add(1)
	go execute_cmd(LeaderId1) //restarting the killed servers
	//time.Sleep(time.Second * 2)

	//tester <- true
	//wgroup.Wait()
	conn1.Close()
}

func restartServerAfterSomeTime(LeaderId2 int) {

	time.Sleep(time.Second * 6)
	go execute_cmd(LeaderId2)

}
