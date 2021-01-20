package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

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
}

func Init(appMgrIP string, appMgrPort string) *ClientInfo {
	var ci ClientInfo
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

func (ci *ClientInfo) QueryListFromAppManager() {

	list, err := ci.appManagerService.QueryTaskList(context.Background(), &appcomm.Query{
		ClientId: &appcomm.UUID{Value: strconv.Itoa(1)},
		GeoLocation: &appcomm.Location{
			Lat: 1.1,
			Lon: 1.1,
		},
		AppId: &appcomm.UUID{Value: strconv.Itoa(1)},
	})
	if err != nil {
		log.Println(err)
		return
	}
	taskList := list.GetTaskList()
	ips := [3]string{taskList[0].GetIp(), taskList[1].GetIp(), taskList[2].GetIp()}
	ports := [3]string{taskList[0].GetPort(), taskList[1].GetPort(), taskList[2].GetPort()}
	if ci.serverIPs[0] == "" {
		ci.serverIPs = ips
		ci.serverPorts = ports
		ci.taskIP = ips[0]
		ci.taskPort = ports[0]
		return
	}
	for i := 0; i < nMultiConn; i++ {
		found := false
		for j := 0; j < len(ips); j++ {
			if ips[j] == ci.serverIPs[i] && ports[j] == ci.serverPorts[i] {
				found = true
				break
			}
		}
		if !found {
			ci.mutexServerUpdate.Lock()
			ci.backupServers[ci.serverIPs[i]+":"+ci.serverPorts[i]] = true
			ci.mutexServerUpdate.Unlock()
		}
	}

	ci.mutexServerUpdate.Lock()
	ci.serverIPs = ips
	ci.serverPorts = ports
	ci.mutexServerUpdate.Unlock()

	// setup conn, service and stream for new server if one exists in the query list
	for i := 0; i < nMultiConn; i++ {
		ci.mutexServerUpdate.Lock()
		if _, ok := ci.conns[ips[i]+":"+ports[i]]; !ok {

			key := ips[i] + ":" + ports[i]
			conn, err := grpc.Dial(key, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Connection to server failed: %v", err)
			}
			ci.conns[key] = conn
			ci.service[key] = clientToTask.NewRpcClientToTaskClient(conn)
			stream, err := ci.service[key].SendRecvImage(context.Background())
			if err != nil {
				log.Fatalf("Client stide creation failed: %v", err)
			}
			ci.stream[key] = stream
		}
		ci.mutexServerUpdate.Unlock()
	}
	ci.mutexBestServer.Lock()
	if ci.taskIP != ips[0] && ci.taskPort != ports[0] {
		fmt.Printf("Changing IP:Port to %s:%s\n", ips[0], ports[0])
		ci.taskIP = ips[0]
		ci.taskPort = ports[0]
		ci.newServer = true
	}
	ci.mutexBestServer.Unlock()
}

func (ci *ClientInfo) PeriodicFuncCalls(wg *sync.WaitGroup) {
	defer wg.Done()
	identifyBestServerTicker := time.NewTicker(5 * time.Second)
	queryListTicker := time.NewTicker(8 * time.Second)

	for {
		select {
		case <-identifyBestServerTicker.C:
			ci.IdentifyBestServer()
		case <-queryListTicker.C:
			ci.QueryListFromAppManager()
		}
	}
}

func (ci *ClientInfo) IdentifyBestServer() {
	// startIBS := time.Now()
	img := gocv.IMRead("data/test/Anthony_Fauci.jpg", gocv.IMReadColor)
	dims := img.Size()
	data := img.ToBytes()
	mattype := int32(img.Type())
	imgData := clientToTask.ImageData{
		Width:   int32(dims[0]),
		Height:  int32(dims[1]),
		MatType: mattype,
		Image:   data,
	}
	prevElapsedTime := time.Duration(10 * time.Second)
	taskIP := ci.taskIP
	taskPort := ci.taskPort
	for i := 0; i < nMultiConn; i++ {
		start := time.Now()
		for j := 0; j < 3; j++ {
			ci.mutexServerUpdate.Lock()
			key := ci.serverIPs[i] + ":" + ci.serverPorts[i]
			ci.mutexServerUpdate.Unlock()
			_, err := ci.service[key].TestPerformance(context.Background(), &imgData)
			if err != nil {
				log.Fatalf("Error sending test image to %s:%v", key, err)
			}
		}
		elapsedTime := time.Since(start)
		if prevElapsedTime > elapsedTime {
			prevElapsedTime = elapsedTime
			ci.mutexServerUpdate.Lock()
			taskIP = ci.serverIPs[i]
			taskPort = ci.serverPorts[i]
			ci.mutexServerUpdate.Unlock()
		}
	}
	ci.mutexBestServer.Lock()
	ci.taskIP = taskIP
	ci.taskPort = taskPort
	ci.newServer = true
	ci.mutexBestServer.Unlock()
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

		ci.mutexBestServer.Lock()
		if ci.newServer {
			taskIP = ci.taskIP
			taskPort = ci.taskPort
			ci.newServer = false
		}
		ci.mutexBestServer.Unlock()
		ci.mutexServerUpdate.Lock()
		stream := ci.stream[taskIP+":"+taskPort]
		ci.mutexServerUpdate.Unlock()

		chunks := split(dataSend, 4096)
		nChunks := len(chunks)

		t1 := time.Now()
		for i := 0; i < nChunks; i++ {
			if i == 0 {
				err = stream.Send(&clientToTask.ImageData{
					Width:   int32(dims[0]),
					Height:  int32(dims[1]),
					MatType: mattype,
					Image:   chunks[i],
					Start:   1,
				})

				if err != nil {
					log.Fatalf("Error sending image frame: %v", err)
				}
			} else if i == nChunks-1 {
				err = stream.Send(&clientToTask.ImageData{
					Start: 0,
					Image: chunks[i],
				})

				if err != nil {
					log.Fatalf("Error sending image frame: %v", err)
				}

			} else {
				err = stream.Send(&clientToTask.ImageData{
					Start: -1,
					Image: chunks[i],
				})

				if err != nil {
					log.Fatalf("Error sending image frame: %v", err)
				}
			}
		}

		dataRecv := make([]byte, 3000000)
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

		fmt.Printf("Frame latency - %v \n", time.Since(t1))
		_, err := gocv.NewMatFromBytes(int(width), int(height), gocv.MatType(matType), dataRecv)
		if err != nil {
			log.Fatalf("Error converting bytes to matrix: %v", err)
		}

		ci.mutexServerUpdate.Lock()
		key := taskIP + ":" + taskPort
		if _, ok := ci.backupServers[taskIP+":"+taskPort]; ok {
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
