package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	guuid "github.com/google/uuid"
	"github.com/nikhs247/objectDetection/comms/rpc/appcomm"
	"github.com/nikhs247/objectDetection/comms/rpc/clientToTask"
	"gocv.io/x/gocv"
	"google.golang.org/grpc"
)

const nMultiConn = 3

type ClientInfo struct {
	// Clinet info
	id       string
	tag      string // used to specify LAN resources
	location *appcomm.Location
	appId    *appcomm.UUID

	// Application manager info
	appManagerConn    *grpc.ClientConn // keep this pointer so that we can close it at the end
	appManagerService appcomm.ApplicationManagerClient

	// Shared data structure
	// Current selected server and 3 server slots
	currentServer int // index of servers
	servers       [nMultiConn]*ServerConnection

	// Lock for shared data structure
	mutexServerUpdate *sync.Mutex
}

// Information required for each server candidate
type ServerConnection struct {
	ip      string
	port    string
	conn    *grpc.ClientConn
	service clientToTask.RpcClientToTaskClient
	stream  clientToTask.RpcClientToTask_SendRecvImageClient
}

func Init(appMgrIP string, appMgrPort string, where string, tag string) *ClientInfo {

	// (1) Set up client info
	clientId := guuid.New().String()

	var loc *appcomm.Location
	if where == "Minneapolis" { // Minneapolis: close
		loc = &appcomm.Location{
			Lat: 44.98,
			Lon: -93.24,
		}
	} else if where == "Duluth" { // Duluth: far
		loc = &appcomm.Location{
			Lat: 46.79,
			Lon: -92.11,
		}
	} else {
		loc = &appcomm.Location{ // Rochester: other
			Lat: 44.02,
			Lon: -92.47,
		}
	}
	// which application this client is requesting for
	whichApp := &appcomm.UUID{Value: strconv.Itoa(1)}

	// (2) Build up connection to Application Manager
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	appConn, err := grpc.Dial(appMgrIP+":"+appMgrPort, opts...)
	// TODO: appConn.Close() in main thread at the end
	if err != nil {
		log.Println("Fail to connect appManager at the beginning")
		os.Exit(0)
	}
	appService := appcomm.NewApplicationManagerClient(appConn)

	// (3) Get initial server list from Application Manager
	list, err := appService.QueryTaskList(context.Background(), &appcomm.Query{
		ClientId:    &appcomm.UUID{Value: clientId},
		GeoLocation: loc,
		Tag:         []string{tag},
		AppId:       whichApp,
	})
	if err != nil {
		log.Println("Fail to query appManager at the beginning")
		os.Exit(0)
	}
	taskList := list.GetTaskList()

	// (4) Build up connections to 3 initial servers
	var servers [nMultiConn]*ServerConnection
	for i := 0; i < nMultiConn; i++ {
		serverIp := taskList[i].GetIp()
		serverPort := taskList[i].GetPort()
		serverConn, err := grpc.Dial(serverIp+":"+serverPort, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Build initial connections to 3 servers failed: %v", err)
		}
		serverService := clientToTask.NewRpcClientToTaskClient(serverConn)
		serverStream, err := serverService.SendRecvImage(context.Background())
		if err != nil {
			log.Fatalf("Build initial connections to 3 servers failed: %v", err)
		}
		servers[i] = &ServerConnection{
			ip:      serverIp,
			port:    serverPort,
			conn:    serverConn,
			service: serverService,
			stream:  serverStream,
		}
	}

	// (5) Construct the ClientInfo
	ci := &ClientInfo{
		// Client info
		id:       clientId,
		tag:      tag,
		location: loc,
		appId:    whichApp,
		// Application Manager info
		appManagerConn:    appConn,
		appManagerService: appService,
		// Shared data structure
		currentServer: 0,
		servers:       servers,
		// Lock
		mutexServerUpdate: &sync.Mutex{},
	}
	return ci
}

func (ci *ClientInfo) QueryListFromAppManager() {
	/////////////////////////////////////////////////////// (1) Get taskList from Application Manager

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
	taskList := list.GetTaskList()

	/////////////////////////////////////////////////////// (2) Construct the list for perfromance test

	ci.mutexServerUpdate.Lock()
	currentServer_tmp := ci.currentServer // this is unsafe because pointers points to shared memory: It's ok because we only read
	servers_tmp := ci.servers
	ci.mutexServerUpdate.Unlock()

	testList := make([]*ServerConnection, 0)
	garbageList := make([]*ServerConnection, 0)

	// Add failed servers into garbage list
	for i := 0; i < currentServer_tmp; i++ {
		garbageList = append(garbageList, servers_tmp[i])
	}
	// Add current servers into test list
	// Note that testList[0] will always be the currently using server in main thread
	for i := currentServer_tmp; i < nMultiConn; i++ {
		testList = append(testList, servers_tmp[i])
	}
	existingNumberOfServers := len(testList)

	// Add new servers from query into test list
Loop1:
	for i := 0; i < nMultiConn; i++ {
		serverIp := taskList[i].GetIp()
		serverPort := taskList[i].GetPort()
		// If new server is an existing server, then skip it
		for j := 0; j < existingNumberOfServers; j++ {
			// if serverIp == testList[j].ip && serverPort == testList[j].port {
			if serverIp == testList[j].ip {
				continue Loop1
			}
		}
		// This is a new server different from the exsiting servers ==> create connection to it
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

	/////////////////////////////////////////////////////// (3) Performance test for servers in test list
	// Mark the currently using server: index in testList
	indexOfCurrentServer := -1
	var currentServerPerformance time.Duration
	// List for sorting
	sortList := make(PairList, 0)

	currentConnectedNodeProbeFinish := false
Loop2:
	for i := 0; i < len(testList); i++ {
		// Check if the test server is the one currently connected
		selfCheck := false
		if i == 0 || !currentConnectedNodeProbeFinish {
			selfCheck = true
		}

		// Stores cumulative time for 3 performance test calls for each server
		t1 := time.Now()
		for j := 0; j < 3; j++ {
			_, err := testList[i].service.TestPerformance(context.Background(), &clientToTask.TestPerf{
				Check:    selfCheck,
				ClientID: ci.id,
			})
			if err != nil {
				// Server failed during test performance ==> remove this server from candidate list
				// Note: this error rarely happens because all elements in test list are valid a couple of microseconds ago
				garbageList = append(garbageList, testList[i])
				testList[i] = nil
				continue Loop2
			}

			// TODO: NEED to consider the bandwidth simulation
			// NOW probing only considers latency + execute dummy data
		}
		currentConnectedNodeProbeFinish = true

		// MIST client
		if testList[i].ip == "128.101.118.101" {
			time.Sleep(time.Duration(rand.Float32()*3+10) * time.Millisecond * 3)
		}
		t2 := time.Now()
		consumed := t2.Sub(t1)
		// Add valid server into sort list for sorting
		sortList = append(sortList, Pair{i, consumed})

		// Print out the performance probing result
		fmt.Printf("? -------- Probing %s:%s %v\n", testList[i].ip, testList[i].port, consumed)

		// First non-nil server in testList is the currently using server
		// Normal case, current server is just testList[0]. But in case the server faliure happens during performance test
		if indexOfCurrentServer == -1 {
			indexOfCurrentServer = i
			currentServerPerformance = t2.Sub(t1)
		}
	}

	// Check if the number of available servers are enough to support this user
	if len(sortList) < 3 {
		log.Println("Available servers are less than 3. Client aborts!")
		os.Exit(0)
	}
	// Sort the available servers based on performance test
	sort.Sort(sortList)

	/////////////////////////////////////////////////////// (4) Construct the new server list for main thread

	var newServers [nMultiConn]*ServerConnection
	grace, err := time.ParseDuration("20ms")
	if err != nil {
		panic(err)
	}

	index := 0 // Index in sortList: for closing connection
	connectionSwitch := false

	if sortList[0].Value+grace < currentServerPerformance {
		// Connection switch is required ==> just use the first 3 servers in sort list
		connectionSwitch = true
		for i := 0; i < nMultiConn; i++ {
			newServers[i] = testList[sortList[i].Index]
		}
		index = 3
	} else {
		// No connection switch is required ==> still use currentServer as the first one and fill the rest 2 slots with sortList
		newServers[0] = testList[indexOfCurrentServer]
		for i := 1; i < nMultiConn; i++ {
			if sortList[index].Index == indexOfCurrentServer {
				index++
			}
			newServers[i] = testList[sortList[index].Index]
			index++
		}
	}

	// Add the rest connections in testing phase into garbage pool
	for i := index; i < len(sortList); i++ {
		// avoid to close the currently used server (senario: previously used server is slower no more than 15ms (no connection switch) but ranked at bottom in sortList)
		if !connectionSwitch && sortList[i].Index == indexOfCurrentServer {
			continue
		}
		garbageList = append(garbageList, testList[sortList[i].Index])
	}

	/////////////////////////////////////////////////////// (5) Update the server list in mainthread

	ci.mutexServerUpdate.Lock()
	ci.servers = newServers
	ci.currentServer = 0
	ci.mutexServerUpdate.Unlock()

	/////////////////////////////////////////////////////// (6) Clean up the garbage pool

	// Close the unused file descriptors explicitely
	// Apply a short delay before closing conns to avoid the following case (rarely happen):
	// 		Close a currently using server in main thread before main thread enters the next critical section
	// 		This will cause a false server failure and trigger fault tolerance mechanism in main thread
	// This problem exists for both sequential send/recv and asynchronous send/recv. We can solve it by simply delaying conn termination
	go func() {
		time.Sleep(1 * time.Second)
		for i := 0; i < len(garbageList); i++ {
			garbageList[i].conn.Close()
			// the rest will be cleaned by Garbage Collector
		}
	}()
}

func (ci *ClientInfo) PeriodicFuncCalls() {
	queryListTicker := time.NewTicker(5 * time.Second)
	// the period of periodic query is [5 - 7] seconds
	for {
		select {
		case <-queryListTicker.C:
			rand.Seed(time.Now().UTC().UnixNano())
			time.Sleep(time.Duration(rand.Float32()*2) * (time.Second))
			ci.QueryListFromAppManager()
		}
	}
}

func (ci *ClientInfo) faultTolerance() {
	ci.mutexServerUpdate.Lock()
	ci.currentServer++
	if ci.currentServer >= 3 {
		log.Println("All 3 servers failed: no available servers and abort")
		// Note: This will rarely happen if we add more duplicated connections
		// Now for simplicity, we assume all 3 servers will not fail at the same time
		os.Exit(0)
	}
	ci.mutexServerUpdate.Unlock()
	fmt.Println("! Server just failed: switch to a backup server!!")
}

func (ci *ClientInfo) StartStreaming() {

	// Set up video source [camera or video file]
	videoPath := "video/vid.avi"
	video, err := gocv.VideoCaptureFile(videoPath)
	if err != nil {
		log.Printf("Error opening video capture file: %s\n", videoPath)
		return
	}
	defer video.Close()
	img := gocv.NewMat()
	defer img.Close()

	// Main loop for client: send frames out to server and get results
	// Server may fail: there are 3 Send() and 1 Recv() functions could lead to error during data transfer
	// Call faultTolerance() to handle connection switch

	// stream is the local variable for currently selected server
	var stream clientToTask.RpcClientToTask_SendRecvImageClient
Loop:
	for {
		// (1) Capture the frame at this iteration to be sent
		if ok := video.Read(&img); !ok {
			fmt.Printf("Video closed: %v\n", videoPath)
			break
		}
		if img.Empty() {
			continue
		}

		// (2) Get the server for processing this frame
		// Lock 1
		ci.mutexServerUpdate.Lock()
		whichIp := ci.servers[ci.currentServer].ip // this is used for emulated data
		stream = ci.servers[ci.currentServer].stream
		ci.mutexServerUpdate.Unlock()

		// (3) Send the frame
		// Encode this frame
		dataSend, _ := gocv.IMEncode(".jpg", img)
		chunks := split(dataSend, 4096)
		nChunks := len(chunks)

		// Timer for frame latency
		t1 := time.Now()

		for i := 0; i < nChunks; i++ {
			// Send the header of this frame
			if i == 0 {
				err = stream.Send(&clientToTask.ImageData{
					Start:    1, // header
					Image:    chunks[i],
					ClientID: ci.id,
				})
				if err != nil {
					ci.faultTolerance()
					continue Loop
				}
				// Send the last chunk
			} else if i == nChunks-1 {
				err = stream.Send(&clientToTask.ImageData{
					Start:    0,
					Image:    chunks[i],
					ClientID: ci.id,
				})
				if err != nil {
					ci.faultTolerance()
					continue Loop
				}
				// Send the regular chunk
			} else {
				err = stream.Send(&clientToTask.ImageData{
					Start:    -1, // regular chunck
					Image:    chunks[i],
					ClientID: ci.id,
				})
				if err != nil {
					ci.faultTolerance()
					continue Loop
				}
			}
		}

		// (4) Receive the result
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			ci.faultTolerance()
			continue Loop
		}

		// ADD MIST node to MIST server Latency

		if whichIp == "128.101.118.101" {
			// time.Sleep(13 * time.Millisecond)
			time.Sleep(time.Duration(rand.Float32()*3+10) * time.Millisecond)
		}
		t2 := time.Now()

		// captainName := "wrong address"
		// if whichIp == "75.72.180.105" {
		// 	captainName = "Sumanth"
		// } else if whichIp == "34.239.128.51" {
		// 	captainName = "captain2"
		// } else if whichIp == "35.170.192.158" {
		// 	captainName = "captain3"
		// } else if whichIp == "3.88.85.7" {
		// 	captainName = "captain4"
		// } else if whichIp == "52.87.164.149" {
		// 	captainName = "captain5"
		// } else if whichIp == "54.172.153.187" {
		// 	captainName = "captain6"
		// } else if whichIp == "54.172.153.187" {
		// 	captainName = "captain7"
		// } else if whichIp == "54.172.153.187" {
		// 	captainName = "captain8"
		// }

		// Print out the frame latency
		// log.Printf("%s %v \n", captainName, t2.Sub(t1))
		fmt.Printf("* %s %v \n", whichIp, t2.Sub(t1))
	}
}

func main() {

	if len(os.Args) != 5 {
		log.Println("Not enough parameters: [AM IP] [AM port] [location: which city] [tag]")
		return
	}
	appMgrIP := os.Args[1]
	appMgrPort := os.Args[2]
	loc := os.Args[3]
	tag := os.Args[4]

	ci := Init(appMgrIP, appMgrPort, loc, tag)

	// Periodic query
	go ci.PeriodicFuncCalls()

	// Main thread
	ci.StartStreaming()

	log.Println("Processing done!")
}

////////////////////////////////////////////////// Helper /////////////////////////////////////////////////////

type Pair struct {
	Index int
	Value time.Duration
}

type PairList []Pair

func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func split(buf []byte, lim int) [][]byte {
	var chunk []byte
	chunks := make([][]byte, 0, len(buf)/lim+1)
	for len(buf) >= lim {
		chunk, buf = buf[:lim], buf[lim:]
		chunks = append(chunks, chunk)
	}
	if len(buf) > 0 {
		chunks = append(chunks, buf[:])
	}
	return chunks
}
