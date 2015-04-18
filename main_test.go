package main

import (
	//"bufio"
	//"bytes"
	"encoding/json"
	//"fmt"
	//"io"
	"io/ioutil"
	//"math/rand"
	//"net"
	"os"
	"os/exec"
	"raft"
	"strconv"
	//"strings"
	"sync"
	//	"testing"
	"time"
)

var noOfThreads int = 500
var noOfRequestsPerThread int = 10
var wgroup sync.WaitGroup
var commands []string
var procmap map[int]*exec.Cmd

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
	for i := 1; i <= no_servers; i++ {
		wgroup.Add(1)
		go execute_cmd(i)
	}

	wgroup.Wait()

}

func execute_cmd(id int) {
	//fmt.Println("Server Id = ", id)
	cmd := exec.Command("./bin/kvstore", "-id="+strconv.Itoa(id))

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	procmap[id] = cmd
	err := cmd.Run()
	if err != nil {
		wgroup.Done()
	}

	cmd.Wait()
	wgroup.Done()

}

//..............................................................................................................................//

// Testcase to test basic Set and Get operation

//..............................................................................................................................//

/*
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

	conn, error := net.Dial("tcp", "127.0.0.1:9001")
	if error != nil {
		tester <- false
		conn.Close()
	}

	io.Copy(conn, bytes.NewBufferString("set Roll101 50 5\r\nTarun\r\n"))

	reader := bufio.NewReader(conn)
	resp, error := reader.ReadBytes('\n')
	//fmt.Println(res)
	res := string(resp)
	if error != nil {
		conn.Close()
	} else {
		if len(res) > 0 {
			response := strings.Split(strings.TrimSpace(res), " ")
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
	//fmt.Println(res)
	res = string(resp)
	if error != nil {
		conn.Close()
	} else {
		response := strings.Split(strings.TrimSpace(res), " ")
		if response[0] == "VALUE" || response[1] == "5" {

			res, error = reader.ReadString('\n')
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
*/
/*
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

	conn, error := net.Dial("tcp", "127.0.0.1:9001")
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

	conn, error := net.Dial("tcp", "127.0.0.1:9001")
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

// Testcase: To test error condition of Error-Redirect

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

	conn, error := net.Dial("tcp", "127.0.0.1:9002")
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

	io.Copy(conn, bytes.NewBufferString("set key 50 2\r\nab\r\n"))
	res, err = reader.ReadBytes('\n')
	if err != nil || string(res) != "Redirect to server 1\r\n" {
		tester <- false
		conn.Close()
	}
	conn.Close()

	conn, error = net.Dial("tcp", "127.0.0.1:9003")
	if error != nil {
		tester <- false
		conn.Close()
	}
	reader = bufio.NewReader(conn)

	io.Copy(conn, bytes.NewBufferString("get key\r\n"))
	res, err = reader.ReadBytes('\n')
	if err != nil || string(res) != "Redirect to server 1\r\n" {
		tester <- false
	}
	conn.Close()

	conn, error = net.Dial("tcp", "127.0.0.1:9004")
	if error != nil {
		tester <- false
		conn.Close()
	}
	reader = bufio.NewReader(conn)

	io.Copy(conn, bytes.NewBufferString("getm key\r\n"))
	res, err = reader.ReadBytes('\n')
	if err != nil || string(res) != "Redirect to server 1\r\n" {
		tester <- false
	}
	conn.Close()

	conn, error = net.Dial("tcp", "127.0.0.1:9005")
	if error != nil {
		tester <- false
		conn.Close()
	}
	reader = bufio.NewReader(conn)

	io.Copy(conn, bytes.NewBufferString("delete key\r\n"))
	res, err = reader.ReadBytes('\n')
	if err != nil || string(res) != "Redirect to server 1\r\n" {
		tester <- false
		conn.Close()
	}

	io.Copy(conn, bytes.NewBufferString("cas ROLL2 500 1 5\r\nabcde\r\n"))
	res, err = reader.ReadBytes('\n')
	if err != nil || string(res) != "Redirect to server 1\r\n" {
		tester <- false
	}
	conn.Close()

	tester <- true

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

	conn, error := net.Dial("tcp", "127.0.0.1:9001")
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

	conn, error := net.Dial("tcp", "127.0.0.1:9001")
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

	conn, error := net.Dial("tcp", "127.0.0.1:9001")
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

//..............................................................................................................................//

// Testcase: To test whether multiple clients works together properly.

//..............................................................................................................................//

func TestMulticlient(t *testing.T) {

	tester := make(chan int)

	for i := 0; i < noOfThreads; i++ {
		go multiclient(t, tester)
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

func multiclient(t *testing.T, tester chan int) {

	conn, err := net.Dial("tcp", "127.0.0.1:9001")
	if err != nil {
		conn.Close()
	}
	reader := bufio.NewReader(conn)
	io.Copy(conn, bytes.NewBufferString("set key 500 2\r\ntj\r\n"))
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

//..............................................................................................................................//

//TestConcurrentMulti() tests ability of system to sustain multiple commands from multiple clients and ensures clients are responded with proper response to clients.

//..............................................................................................................................//
func TestConcurrentMulti(t *testing.T) {

	commands = []string{
		"set DUMMYKEY 15 10\r\nDUMMYVALUE\r\n",
		"set DUMMYKEY2 15 10\r\nDUMMYVALUE\r\n",
		"delete DUMMYKEY2\r\n",
		"delete DUMMYKEY\r\n",
		"getm DUMMYKEY2\r\n",
	}

	tester := make(chan bool)

	i := 0
	for i < noOfThreads {
		go CheckConcurrentMulti(t, tester)
		i += 1
	}
	i = 0

	for i < noOfThreads {
		<-tester
		i += 1
	}

}

func CheckConcurrentMulti(t *testing.T, tester chan bool) {
	conn, err := net.Dial("tcp", "127.0.0.1:9001")
	if err != nil {
		t.Error(err)
		tester <- true
		return
	}
	reader := bufio.NewReader(conn)
	i := 0
	randomGenerator := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i < noOfRequestsPerThread {
		index := randomGenerator.Int() % len(commands)
		io.Copy(conn, bytes.NewBufferString(commands[index]))

		data, _ := reader.ReadBytes('\n')
		input := strings.TrimRight(string(data), "\r\n")
		array := strings.Split(input, " ")
		result := array[0]
		if result == "VALUE" {
			data, _ = reader.ReadBytes('\n')
			input := strings.TrimRight(string(data), "\r\n")
			temp, _ := strconv.Atoi(array[2])
			if input != "DUMMYVALUE" && temp <= 15 {

				t.Fail()
			}

		} else {

			if result != "ERRNOTFOUND" && result != "ERR_VERSION" && result != "DELETED" && result != "OK" {

				t.Fail()
			}
		}

		i = i + 1
	}
	tester <- true
	conn.Close()
}
//..............................................................................................................................//

//TestConsensusArrived() tests ability of system to to repetedly call append() method until consensus is arrived.
//For this test we are kill majority servers for some time and then relaunching them.

//..............................................................................................................................//


func TestConsensusArrived(t *testing.T) {
	tester := make(chan bool)
	go checkConsensusArrived(t, tester)
	check := <-tester

	if !check {
		t.Error("TestConsensusArrived Failed, please check implementation")
	} else {

		fmt.Println("Testcase - TestConsensusArrived : Passed")
	}
}
func checkConsensusArrived(t *testing.T,tester chan bool) {

conn, error := net.Dial("tcp", "127.0.0.1:9001")
	if error != nil {
		tester <- false
		conn.Close()
	}

	io.Copy(conn, bytes.NewBufferString("set Roll101 50 5\r\nTarun\r\n"))

	reader := bufio.NewReader(conn)
	resp, error := reader.ReadBytes('\n')
	//fmt.Println(res)
	res := string(resp)
	if error != nil {
		conn.Close()
	} else {
		if len(res) > 0 {
			response := strings.Split(strings.TrimSpace(res), " ")
			if response[0] != "OK" {
				tester <- false
				conn.Close()
			}

		} else {
			tester <- false
			conn.Close()
		}

	}


slice := []int{3, 4, 5}

 for _,elem := range slice{
	if err := procmap[elem].Process.Kill(); err != nil {
            t.Log("failed to kill: ", err)
        }
 	t.Log("YOOYOYOYOYOMMMMMMMM")
  	wgroup.Done()
 }

time.Sleep(100 * time.Second)



io.Copy(conn, bytes.NewBufferString("get Roll101\r\n"))
	resp, error = reader.ReadBytes('\n')
	//fmt.Println(res)
	res = string(resp)
	if error != nil {
		conn.Close()
	} else {
		response := strings.Split(strings.TrimSpace(res), " ")
		if response[0] == "VALUE" || response[1] == "5" {

			res, error = reader.ReadString('\n')
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

func TestKillAllServers(t *testing.T) {
	cmd := exec.Command("pkill", "kvstore")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		t.Log(err)
	}
	cmd.Wait()

}
*/
