package objectdetectionclient

import (
	"context"
	"errors"
	"log"
	"os"
	"sort"
	"time"

	"github.com/nikhs247/objectDetection/comms/rpc/appcomm"
	"github.com/nikhs247/objectDetection/comms/rpc/clientToTask"
	"google.golang.org/grpc"
)

func (ci *ClientInfo) queryAppManager() []*appcomm.Task {
	list, err := ci.appManagerService.QueryTaskList(context.Background(), &appcomm.Query{
		ClientId:    &appcomm.UUID{Value: ci.id},
		GeoLocation: ci.location,
		Tag:         []string{},
		AppId:       ci.appId,
	})
	if err != nil {
		log.Println("Application manager fails")
		os.Exit(0)
	}
	return list.GetTaskList()
}

func constructTestListInit(taskList []*appcomm.Task) []*ServerConnection {
	testList := make([]*ServerConnection, 0)

	// Simply establish connections to all nodes from appManager query
	for i := 0; i < len(taskList); i++ {
		serverIp := taskList[i].GetIp()
		serverPort := taskList[i].GetPort()
		serverConn, err := grpc.Dial(serverIp+":"+serverPort, grpc.WithInsecure())
		if err != nil {
			// This new server is already failed ==> do nothing about it
			continue
		}
		serverService := clientToTask.NewRpcClientToTaskClient(serverConn)
		serverStream, err := serverService.SendRecvImage(context.Background())
		if err != nil {
			// This new server is already failed ==> do nothing about it
			serverConn.Close()
			continue
		}
		newServer := &ServerConnection{
			ip:      serverIp,
			port:    serverPort,
			conn:    serverConn,
			service: serverService,
			// stream will not be used by performance test, but we still create it here in case this server is chosen
			stream: serverStream,
		}
		// Add the new server into test list
		testList = append(testList, newServer)
	}
	return testList
}

func constructTestList(taskList []*appcomm.Task, currentServer_tmp int, servers_tmp []*ServerConnection) ([]*ServerConnection, []*ServerConnection) {
	testList := make([]*ServerConnection, 0)
	garbageList := make([]*ServerConnection, 0)

	// Add failed servers into garbage list
	for i := 0; i < currentServer_tmp; i++ {
		garbageList = append(garbageList, servers_tmp[i])
	}

	// Add the currently-using server (real traffic, not backup servers) into the test list
	// Always include currently-using server in the probing list (to avoid unnecessary connection switch)
	// Note that testList[0] will always be the currently using server in main thread
	// It's ok testList[0] fails during the probing process
	testList = append(testList, servers_tmp[currentServer_tmp])

	// Handle the rest of old server list: discard or keep it? (if overlap with the new server list)
Loop1:
	for i := currentServer_tmp + 1; i < len(servers_tmp); i++ {
		serverIp := servers_tmp[i].ip
		serverPort := servers_tmp[i].port
		for j := 0; j < len(taskList); j++ {
			if serverIp == taskList[j].GetIp() && serverPort == taskList[j].GetPort() {
				testList = append(testList, servers_tmp[i])
				continue Loop1
			}
		}
		// This old server doesn't overlap with any new server in the list
		garbageList = append(garbageList, servers_tmp[i])
	}

Loop2:
	// Add new servers from query into test list
	for i := 0; i < len(taskList); i++ {
		serverIp := taskList[i].GetIp()
		serverPort := taskList[i].GetPort()

		// If this one is the currently-using server itself -> continue it
		// We already handled the current server above
		if serverIp == testList[0].ip && serverPort == testList[0].port {
			continue
		}

		// Test if this server exists in the old server list
		for j := currentServer_tmp + 1; j < len(servers_tmp); j++ {
			if serverIp == servers_tmp[j].ip && serverPort == servers_tmp[j].port {
				// We already append this server in Loop1: just skip it here
				continue Loop2
			}
		}

		// This is a new server different from the exsiting servers: establish connection to it
		serverConn, err := grpc.Dial(serverIp+":"+serverPort, grpc.WithInsecure())
		if err != nil {
			// This new server is already failed ==> do nothing about it
			continue
		}
		serverService := clientToTask.NewRpcClientToTaskClient(serverConn)
		serverStream, err := serverService.SendRecvImage(context.Background())
		if err != nil {
			// This new server is already failed ==> do nothing about it
			serverConn.Close()
			continue
		}
		newServer := &ServerConnection{
			ip:      serverIp,
			port:    serverPort,
			conn:    serverConn,
			service: serverService,
			// stream will not be used by performance test, but we still create it here in case this server is chosen
			stream: serverStream,
		}
		// Add the new server into test list
		testList = append(testList, newServer)
	}
	return testList, garbageList
}

func (ci *ClientInfo) constructSortList(testList []*ServerConnection, garbageList []*ServerConnection) (PairList, []*ServerConnection, []*ServerConnection, bool, time.Duration) {
	sortList := make(PairList, 0)
	// This flag is used to indicate if the currently-using server is still alive
	currentlyUseingServerAlive := true
	var currentlyUseingServerPerformance time.Duration

	for i := 0; i < len(testList); i++ {
		// Perform the rtt test
		t1 := time.Now()
		_, err := testList[i].service.RTT_Request(context.Background(), &clientToTask.TestPerf{
			ClientID: ci.id,
		})
		if err != nil {
			// This error rarely happens -> Server just fails during the probing process ->remove it
			if i == 0 {
				currentlyUseingServerAlive = false
			}
			garbageList = append(garbageList, testList[i])
			testList[i] = nil
			continue
		}
		rtt_time := time.Since(t1)

		// Perform the processing test
		probeResult, err := testList[i].service.Probe_Request(context.Background(), &clientToTask.TestPerf{
			ClientID: ci.id,
		})
		if err != nil {
			// This error rarely happens -> Server just fails during the probing process ->remove it
			if i == 0 {
				currentlyUseingServerAlive = false
			}
			garbageList = append(garbageList, testList[i])
			testList[i] = nil
			continue
		}
		testList[i].state = probeResult.StateNumber
		process_time := probeResult.WhatIfTime.AsDuration()
		performance := rtt_time + process_time
		if i == 0 {
			currentlyUseingServerPerformance = performance
		}
		sortList = append(sortList, Pair{i, performance})
	}
	// Compare the performance of these servers
	sort.Sort(sortList)

	return sortList, testList, garbageList, currentlyUseingServerAlive, currentlyUseingServerPerformance
}

func constructNewCandidateList(sortList PairList, testList []*ServerConnection, garbageList []*ServerConnection, currentlyUseingServerAlive bool, currentlyUseingServerPerformance time.Duration, currentServer_tmp int) ([]*ServerConnection, []*ServerConnection, error) {
	newServers := make([]*ServerConnection, 0)

	if currentlyUseingServerAlive && currentServer_tmp != -1 {
		if len(sortList) >= 2 { // Need to compare the performance for connection switch
			grace, err := time.ParseDuration("20ms")
			if err != nil {
				panic(err)
			}
			if sortList[0].Value+grace < currentlyUseingServerPerformance {
				// Connection switch
				for i := 0; i < len(sortList); i++ {
					if len(newServers) >= nMultiConn {
						// len(sortList) could have 1 more element than nMultiConn
						garbageList = append(garbageList, testList[sortList[i].Index])
						break
					}
					newServers = append(newServers, testList[sortList[i].Index])
				}
				// Ask for join the new node
				joinResult, err := newServers[0].service.Join_Request(context.Background(), &clientToTask.Decision{
					LastSate: newServers[0].state,
				})
				if err != nil { // Rarely happen: for simplicity just return failure
					return nil, nil, errors.New("probing fails")
				}
				if !joinResult.Success {
					return nil, nil, errors.New("join request rejected")
				}
				// Notify leave
				testList[0].service.EndProcess(context.Background(), &clientToTask.EmptyMessage{})
			} else {
				// No connection switch
				newServers = append(newServers, testList[0])
				for i := 0; i < len(sortList); i++ {
					if len(newServers) >= nMultiConn {
						// len(sortList) could have 1 more element than nMultiConn
						garbageList = append(garbageList, testList[sortList[i].Index])
						break
					}
					if sortList[i].Index == 0 {
						continue // This is the current server: already added
					}
					newServers = append(newServers, testList[sortList[i].Index])
				}
			}
		} else { // Rarely happen: only the current server alive: all others fails (no connection switch)
			newServers = append(newServers, testList[0])
		}
	} else {
		for i := 0; i < len(sortList); i++ { // The length must be <= nMultiConn
			newServers = append(newServers, testList[sortList[i].Index])
		}
		// Ask for join
		joinResult, err := newServers[0].service.Join_Request(context.Background(), &clientToTask.Decision{
			LastSate: newServers[0].state,
		})
		if err != nil { // Rarely happen: for simplicity just return failure
			return nil, nil, errors.New("probing fails")
		}
		if !joinResult.Success {
			return nil, nil, errors.New("join request rejected")
		}
	}
	return newServers, garbageList, nil
}

func cleanUp(garbageList []*ServerConnection) {
	// Close the unused file descriptors explicitely
	// Apply a short delay before closing conns to avoid the following case (rarely happen):
	// 		Close a currently using server in main thread before main thread enters the next critical section
	// 		This will cause a false server failure and trigger fault tolerance mechanism in main thread
	// This problem exists for both sequential send/recv and asynchronous send/recv. We can solve it by simply delaying conn termination
	time.Sleep(1 * time.Second)
	for i := 0; i < len(garbageList); i++ {
		garbageList[i].conn.Close()
		// the rest will be cleaned by Garbage Collector
	}
}

type Pair struct {
	Index int
	Value time.Duration
}

type PairList []Pair

func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
