# Replicated Key Value Store

In this implementation, we have developed a replicated store shared by multiple servers, with the help of a shared log structure and majority consensus based commit.

In this architecture, there are multiple server machines containing the replica of the store, which accept commands from the client. One of the replica is predefined as a leader. The client sends the command to the servers, which processes it only if it is the leader.

The leader, on receiving the request, sends an append message to the other servers. Then, it waits for acknowledgement from the other servers. If it receives acknowledgement from a majority of the servers, it performs the operation on the Key Value Store and responds to the client.

.. (Reference: In Search of an Understandable Consensus Algorithm)

## Run Instructions:

Below command can be used to run the servers individually and clients have to be run separately (telnet can be used)
 
Assuming $GOPATH is set to root directory of project
```
./install.sh

bin/kvstore -id=<server_id>

#e.g.
bin/kvstore -id=1
bin/kvstore -id=2
bin/kvstore -id=3
bin/kvstore -id=4
bin/kvstore -id=5
/* Here <server_id> represents id of the server*/
```





### Test Instruction:
Assuming you are at root folder
Key-Value server can be tested by hitting below command 

```cmd
./install.sh
go test
```

To display which all tests are running, one can use below command

```cmd
./install.sh
go test -v
```


## Work Completed

- Implemented basic operations set,get,getm,delete,cas for Key-Value store
- Handled concurrency using locks
- Used priority queue(MIN-HEAP) for periodic cleanup of expired key-value pairs
  using 'container/heap' package
  
 ( Package heap provides heap operations for any type that implements heap.Interface. 
 A heap is a tree with the property that each node is the minimum-valued node in its subtree.
 A heap is a common way to implement a priority queue. To build a priority queue,
 implement the Heap interface with the (negative) priority as the ordering for the
 Less method, so Push adds items while Pop removes the highest-priority item from
 the queue.The Examples include such an implementation; the file example_pq_test.go
 has the complete source)
- Implemented multiple replica servers with single predefined leader
- Implemented connection handler to parse commands and append to shared log
- Committing on majority consensus
- Tested code on 
	1. Functionality for all the basic functions.
	2. Expiry of key-value pair
	3. Tested concurrent set operaton by multiple cients and at last validated result by comparing version number
	4. Stress testing by sending multiple commands from from clients concurrently


## Requirements

- go 1.4.1 and higher
- zmq 4.1.0

## Features

- Serves more than one million requests in 15 seconds i.e it can handle more than 10000 clients at a time and each client sending 100 requests
- Automatic garbage collection is performed per 5 seconds and this time is tunable
- Various commands supported (mentioned in functionalities) which gives best possible functions out of key value store
- Scalable to addition of servers by modifying configuration file


## Known Bugs

- As of now first server in the configuration file is assumed to be leader.
- Reading the value as a string, instead of byte array.
- Can perform only one append operation at a time


## Functionalities

- The server details are specified in a json file *clusterConfig.json*, which has the content organized as follows:

```json
{
	"Servers":[
	    {"Id":"<server_id>", "Hostname":"<server_hostname>", "ClientPort":"<port_for_client_command>", "LogPort":"<port_for_log_messages>"},
	    ...
	],
	"Count":{"Count":"<total_number_of_servers>"}
}
```

- On receiving the command, the server sends append command to all servers specified in the configuration. It then waits for acknowledgement from the other servers. On receiving *(ClusterSize/2 + 1)* acknowledgements, it writes the command to its log.

- Commands Supported

   1. Set: create the key-value pair, or update the value if it already exists.
   
    ```
    set <key> <exptime> <numbytes>\r\n
    <value bytes>\r\n
    ```

    The server responds with:
    
    ```
    OK <version>\r\n
    ```

    where version is a unique 64-bit number (in decimal format) associated with the key.

   2. Get: Given a key, retrieve the corresponding key-value pair

    ```
    get <key>\r\n
    ```

    The server responds with the following format (or one of the errors described later)

    ```
    VALUE <numbytes>\r\n
    <value bytes>\r\n
    ```

   3. Get Meta: Retrieve value, version number and expiry time left
   
    ```
    getm <key>\r\n
    ```

    The server responds with the following format (or one of the errors described below)
    
    ```
    VALUE <version> <exptime> <numbytes>\r\n
    <value bytes>\r\n
    ```

   4. Compare and swap. This replaces the old value (corresponding to key) with the new value only if the version is still the same.

    ```
    cas <key> <exptime> <version> <numbytes>\r\n
    <value bytes>\r\n
    ```

    The server responds with the new version if successful (or one of the errors described late)

    ```
    OK <version>\r\n
    ```

   5. Delete key-value pair
   
    ```
    delete <key>\r\n
    ```

    Server response (if successful)

    ```
    DELETED\r\n
    ```

### Options:

    1. key : an ascii text string (max 250 bytes) without spaces
    2. numbytes: size of the value block, not including the trailing \r\n. It is in an ascii text format.
    3. version: A 64-bit number generated by the server, in ascii text format.
    4. exptime: An offset in seconds after which the value may not be available. 0 indicates no expiry at all.

### Errors that are returned.

    1. "ERR_VERSION \r\n" (the value was not changed because of a version mismatch)
    2. "ERRNOTFOUND\r\n" (the key doesnâ€™t exist)
    3. "ERRCMDERR\r\n" (the command line is not formatted correctly)
    4. "ERR_INTERNAL\r\n"
    5. "Redirect to server <server>\r\n"
