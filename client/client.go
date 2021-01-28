package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
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
	appManagerIP      string
	appManagerPort    string
	appManagerConn    *grpc.ClientConn
	appManagerService appcomm.ApplicationManagerClient
	serverIPs         [nMultiConn]string
	serverPorts       [nMultiConn]string
	backupServers     map[string]bool
	lastFrameLoc      map[string]int
	conns             map[string]*grpc.ClientConn
	service           map[string]clientToTask.RpcClientToTaskClient
	stream            map[string]clientToTask.RpcClientToTask_SendRecvImageClient
	frameTimer        map[int]time.Time
	mutexBestServer   *sync.Mutex
	mutexTimer        *sync.Mutex
	mutexServerUpdate *sync.Mutex
	taskIP            string
	taskPort          string
	newServer         bool
	id                string
}

func logTime() {
	fmt.Fprintf(os.Stderr, "[%s] ", time.Now().Format("2006-01-02 15:04:05"))
}

func Init(appMgrIP string, appMgrPort string) *ClientInfo {
	var ci ClientInfo
	ci.id = guuid.New().String()
	logTime()
	fmt.Printf("My ID %s\n", ci.id)
	ci.appManagerIP = appMgrIP
	ci.appManagerPort = appMgrPort
	// ci.serverIPs = make([]string, nMultiConn)
	// ci.serverPorts = make([]string, nMultiConn)
	ci.backupServers = make(map[string]bool, nMultiConn)
	ci.conns = make(map[string]*grpc.ClientConn, nMultiConn)
	ci.service = make(map[string]clientToTask.RpcClientToTaskClient, nMultiConn)
	ci.stream = make(map[string]clientToTask.RpcClientToTask_SendRecvImageClient, nMultiConn)
	ci.mutexBestServer = &sync.Mutex{}
	ci.mutexServerUpdate = &sync.Mutex{}
	ci.newServer = false

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(appMgrIP+":"+appMgrPort, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	ci.appManagerConn = conn
	ci.appManagerService = appcomm.NewApplicationManagerClient(conn)

	return &ci
}

type Pair struct {
	Key   string
	Value time.Duration
}

type PairList []Pair

func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func sortTaskInstances(perfTime map[string]time.Duration) PairList {
	pl := make(PairList, len(perfTime))
	i := 0
	for k, v := range perfTime {
		pl[i] = Pair{k, v}
		i++
	}
	sort.Sort(pl)
	return pl
}

func (ci *ClientInfo) QueryListFromAppManager() {

	// get the list of task instance IP:Port from application manager
	list, err := ci.appManagerService.QueryTaskList(context.Background(), &appcomm.Query{
		ClientId: &appcomm.UUID{Value: ci.id},
		GeoLocation: &appcomm.Location{
			Lat: 1.1,
			Lon: 1.1,
		},
		AppId: &appcomm.UUID{Value: strconv.Itoa(1)},
	})
	if err != nil {
		panic(err)
	}

	taskList := list.GetTaskList()
	ips := [3]string{taskList[0].GetIp(), taskList[1].GetIp(), taskList[2].GetIp()}
	ports := [3]string{taskList[0].GetPort(), taskList[1].GetPort(), taskList[2].GetPort()}

	// First time
	if ci.serverIPs[0] == "" {
		ci.serverIPs = ips
		ci.serverPorts = ports
		ci.taskIP = ips[0]
		ci.taskPort = ports[0]
		return
	}

	// backup current information about IP:Port and best IP:Port
	ci.mutexServerUpdate.Lock()
	existingIPs := ci.serverIPs
	existingPorts := ci.serverPorts
	currBestIP := ci.taskIP
	currBestPort := ci.taskPort
	ci.mutexServerUpdate.Unlock()

	// combine current IP:Port set with the one retrieved from application manager
	combinedIPs := make([]string, 0)
	combinedPorts := make([]string, 0)
	for i := 0; i < len(ips); i++ {
		found := false
		for j := 0; j < nMultiConn; j++ {
			if ips[i] == existingIPs[j] && ports[i] == existingPorts[j] {
				found = true
				break
			}
		}
		if !found {
			combinedIPs = append(combinedIPs, ips[i])
			combinedPorts = append(combinedPorts, ports[i])
		}
	}
	combinedIPs = append(combinedIPs, existingIPs[:]...)
	combinedPorts = append(combinedPorts, existingPorts[:]...)

	// stores cumulative time taken by each task instance for 3 performance test calls
	perfTime := make(map[string]time.Duration, nMultiConn)

	// performance test calls
Loop:
	for i := 0; i < len(combinedIPs); i++ {
		key := combinedIPs[i] + ":" + combinedPorts[i]
		available := false

		var service clientToTask.RpcClientToTaskClient
		// check if IP:Port is already available
		ci.mutexServerUpdate.Lock()
		if _, ok := ci.service[key]; ok {
			available = true
			service = ci.service[key]
		}
		ci.mutexServerUpdate.Unlock()

		// if available, then directly call the test performance using stored service handle
		if available {
			start := time.Now()
			for j := 0; j < 3; j++ {
				_, err := service.TestPerformance(context.Background(),
					&clientToTask.TestPerf{
						Check:    true,
						ClientID: ci.id,
					})
				if err != nil {
					// To do -  Fault tolerance
					// ci.mutexServerUpdate.Lock()
					// delete(ci.service, key)
					// delete(ci.stream, key)
					// delete(ci.conns, key)
					// ci.mutexServerUpdate.Unlock()
					continue Loop
				}
			}
			perfTime[key] = time.Since(start)
			logTime()
			fmt.Printf("Time taken available - %v = %v\n", key, perfTime[key])
		} else {
			// if not available, then create connection and service temporarily for performance test
			conn, err := grpc.Dial(key, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Connection to server failed: %v", err)
			}
			service := clientToTask.NewRpcClientToTaskClient(conn)
			start := time.Now()
			for j := 0; j < 3; j++ {
				_, err := service.TestPerformance(context.Background(),
					&clientToTask.TestPerf{
						Check:    true,
						ClientID: ci.id,
					})
				if err != nil {
					// To do - fault tolerance
					continue Loop
				}
			}
			conn.Close()
			perfTime[key] = time.Since(start)
			logTime()
			fmt.Printf("Time taken available - %v = %v\n", key, perfTime[key])
		}
	}

	// sorted list of <<IP:Port>, Duration>
	sortedTaskTimeList := sortTaskInstances(perfTime)
	logTime()
	fmt.Printf("Ordered tasks: %v\n", sortedTaskTimeList)

	// select the best nMultiConn set of IP:Port to connect to
	selectedTaskIter := 0
	for i := 0; i < len(sortedTaskTimeList); i++ {
		available := false
		key := sortedTaskTimeList[i].Key
		ci.mutexServerUpdate.Lock()
		if _, ok := ci.service[key]; ok {
			available = true
		}
		ci.mutexServerUpdate.Unlock()

		splitIpPort := strings.Split(key, ":")
		grace, err := time.ParseDuration("15ms")
		if err != nil {
			panic(err)
		}

		// if the IP:Port is not connected and selected task instances < nMultiConn, create connection, service and stream
		if !available && selectedTaskIter < nMultiConn {
			conn, err := grpc.Dial(key, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Connection to server failed: %v", err)
			}
			service := clientToTask.NewRpcClientToTaskClient(conn)
			stream, err := ci.service[key].SendRecvImage(context.Background())
			if err != nil {
				// To do  - fault tolerance
				log.Fatalf("Client side creation failed: %v", err)
			}
			ci.mutexServerUpdate.Lock()
			ci.conns[key] = conn
			ci.service[key] = service
			ci.stream[key] = stream
			ci.mutexServerUpdate.Unlock()
		}

		// if (!available && selectedTaskIter < nMultiConn) ||
		// 	(available && selectedTaskIter < nMultiConn) {

		if selectedTaskIter < nMultiConn {

			if selectedTaskIter == 0 {
				// check if the existing best task is present in the sortedTaskTimeList
				for k := 0; k < len(sortedTaskTimeList); k++ {
					if sortedTaskTimeList[k].Key == currBestIP+":"+currBestPort {
						// check if k=0 or k's time < new task time + 15 ms grace
						if k == 0 {
							selectedTaskIter++
						} else if sortedTaskTimeList[k].Value < (grace + sortedTaskTimeList[0].Value) {
							// maintain the status of current task and add the new task
							// as second best
							selectedTaskIter++
							ci.mutexServerUpdate.Lock()
							ci.serverIPs[selectedTaskIter] = splitIpPort[0]
							ci.serverPorts[selectedTaskIter] = splitIpPort[1]
							ci.mutexServerUpdate.Unlock()
							selectedTaskIter++
						} else {
							// else add the new task as best
							ci.mutexServerUpdate.Lock()
							ci.serverIPs[selectedTaskIter] = splitIpPort[0]
							ci.serverPorts[selectedTaskIter] = splitIpPort[1]
							ci.taskIP = splitIpPort[0]
							ci.taskPort = splitIpPort[1]
							ci.newServer = true
							ci.mutexServerUpdate.Unlock()
							selectedTaskIter++
						}
						break
					}
				}
			} else {
				// set the IP:Port to selectTaskIter location
				ci.mutexServerUpdate.Lock()
				ci.serverIPs[selectedTaskIter] = splitIpPort[0]
				ci.serverPorts[selectedTaskIter] = splitIpPort[1]
				ci.mutexServerUpdate.Unlock()
				selectedTaskIter++
			}
		} else if available && selectedTaskIter >= nMultiConn {
			logTime()
			fmt.Printf("**************Placing %v in backupserver\n", key)
			ci.mutexServerUpdate.Lock()
			ci.backupServers[key] = true
			ci.mutexServerUpdate.Unlock()
			selectedTaskIter++
		}
	}
	ci.mutexServerUpdate.Lock()
	logTime()
	fmt.Printf("Current IP:Port list ---- %v *** %v", ci.serverIPs, ci.serverPorts)
	logTime()
	fmt.Printf("Current top task instance %v:%v\n", ci.taskIP, ci.taskPort)
	ci.mutexServerUpdate.Unlock()

}

func (ci *ClientInfo) PeriodicFuncCalls(wg *sync.WaitGroup) {
	defer wg.Done()
	queryListTicker := time.NewTicker(5 * time.Second)

	for {
		select {
		case <-queryListTicker.C:
			ci.QueryListFromAppManager()
		}
	}
}

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

func (ci *ClientInfo) StartStreaming(wg *sync.WaitGroup) {
	defer wg.Done()

	taskIP := ci.serverIPs[0]
	taskPort := ci.serverPorts[0]

	// create connection to all nMultConn servers
	for i := 0; i < nMultiConn; i++ {
		key := ci.serverIPs[i] + ":" + ci.serverPorts[i]
		conn, err := grpc.Dial(key, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Connection to server failed: %v", err)
		}
		ci.conns[key] = conn

		ci.service[key] = clientToTask.NewRpcClientToTaskClient(conn)

		stream, err := ci.service[key].SendRecvImage(context.Background())
		if err != nil {
			log.Fatalf("Client stub creation failed: %v", err)
		}
		ci.stream[key] = stream
	}

	// open video to capture
	videoPath := "data/video/vid.avi"
	video, err := gocv.VideoCaptureFile(videoPath)
	if err != nil {
		fmt.Printf("Error opening video capture file: %s\n", videoPath)
		return
	}
	defer video.Close()

	img := gocv.NewMat()
	defer img.Close()

	nImagesSent := 0
	var stream clientToTask.RpcClientToTask_SendRecvImageClient
	ci.mutexServerUpdate.Lock()
	ci.newServer = false
	stream = ci.stream[ci.taskIP+":"+ci.taskPort]
	ci.mutexServerUpdate.Unlock()
	for {

		if ok := video.Read(&img); !ok {
			fmt.Printf("Video closed: %v\n", videoPath)
			break
		}
		if img.Empty() {
			continue
		}

		dims := img.Size()
		dataSend := img.ToBytes()
		mattype := int32(img.Type())

		ci.mutexServerUpdate.Lock()
		if ci.newServer {
			taskIP = ci.taskIP
			taskPort = ci.taskPort
			ci.newServer = false
			stream = ci.stream[taskIP+":"+taskPort]
		}
		ci.mutexServerUpdate.Unlock()

		chunks := split(dataSend, 4096)
		nChunks := len(chunks)

		t1 := time.Now()
		for i := 0; i < nChunks; i++ {
			if i == 0 {
				err = stream.Send(&clientToTask.ImageData{
					Width:    int32(dims[0]),
					Height:   int32(dims[1]),
					MatType:  mattype,
					Image:    chunks[i],
					Start:    1,
					ClientID: ci.id,
				})

				if err != nil {
					log.Fatalf("Error sending image frame: %v", err)
				}
			} else if i == nChunks-1 {
				err = stream.Send(&clientToTask.ImageData{
					Start:    0,
					Image:    chunks[i],
					ClientID: ci.id,
				})

				if err != nil {
					log.Fatalf("Error sending image frame: %v", err)
				}

			} else {
				err = stream.Send(&clientToTask.ImageData{
					Start:    -1,
					Image:    chunks[i],
					ClientID: ci.id,
				})

				if err != nil {
					log.Fatalf("Error sending image frame: %v", err)
				}
			}
		}

		dataRecv := make([]byte, 0)
		var width int32
		var height int32
		var matType int32
		for {
			img, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("Image receive from app failed: %v", err)
			}

			if img.GetStart() == 1 {
				width = img.GetWidth()
				height = img.GetHeight()
				matType = img.GetMatType()
			}

			chunk := img.GetImage()
			dataRecv = append(dataRecv, chunk...)
			if img.GetStart() == 0 {
				break
			}
		}

		logTime()
		fmt.Printf("Frame latency - %v \n", time.Since(t1))
		_, err := gocv.NewMatFromBytes(int(width), int(height), gocv.MatType(matType), dataRecv)
		if err != nil {
			log.Fatalf("Error converting bytes to matrix: %v", err)
		}

		ci.mutexServerUpdate.Lock()
		key := taskIP + ":" + taskPort
		if _, ok := ci.backupServers[taskIP+":"+taskPort]; ok {
			fmt.Printf(" Deleting task instance --- %v:%v\n", taskIP, taskPort)
			ci.conns[key].Close()
			delete(ci.service, key)
			delete(ci.stream, key)
			delete(ci.conns, key)
		}
		ci.mutexServerUpdate.Unlock()

		nImagesSent++
	}
}

func main() {
	appMgrIP := os.Args[1]
	appMgrPort := os.Args[2]

	ci := Init(appMgrIP, appMgrPort)
	ci.QueryListFromAppManager()
	var wg sync.WaitGroup
	wg.Add(1)
	go ci.StartStreaming(&wg)
	wg.Add(1)
	go ci.PeriodicFuncCalls(&wg)
	wg.Wait()
}
