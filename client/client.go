package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/nikhs247/objectDetection/comms/rpc/clientToAppMgr"
	"github.com/nikhs247/objectDetection/comms/rpc/clientToTask"
	"github.com/paulbellamy/ratecounter"
	"gocv.io/x/gocv"
	"google.golang.org/grpc"
)

const nMultiConn = 3

type ClientInfo struct {
	appManagerIP      string
	appManagerPort    string
	appManagerConn    *grpc.ClientConn
	appManagerService clientToAppMgr.ApplicationManagerClient
	serverIPs         [nMultiConn]string
	serverPorts       [nMultiConn]string
	backupServers     map[string]bool
	lastFrameLoc      map[string]int
	conns             map[string]*grpc.ClientConn
	service           map[string]clientToTask.RpcClientToTaskClient
	stream            map[string]clientToTask.RpcClientToTask_SendRecvImageClient
	mutexBestServer   *sync.Mutex
	mutexMapAccess    *sync.Mutex
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
	ci.lastFrameLoc = make(map[string]int, nMultiConn)
	ci.conns = make(map[string]*grpc.ClientConn, nMultiConn)
	ci.service = make(map[string]clientToTask.RpcClientToTaskClient, nMultiConn)
	ci.stream = make(map[string]clientToTask.RpcClientToTask_SendRecvImageClient, nMultiConn)
	ci.mutexBestServer = &sync.Mutex{}
	ci.mutexMapAccess = &sync.Mutex{}
	ci.newServer = false

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	// opts = append(opts, grpc.WithBlock())
	fmt.Println("Dialing to app mgr " + appMgrIP + ":" + appMgrPort)
	conn, err := grpc.Dial(appMgrIP+":"+appMgrPort, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	fmt.Println("Completed connection to app mgr")
	ci.appManagerConn = conn
	ci.appManagerService = clientToAppMgr.NewApplicationManagerClient(conn)

	return &ci
}

func (ci *ClientInfo) QueryListFromAppManager() {

	list, err := ci.appManagerService.QueryTaskList(context.Background(), &clientToAppMgr.Query{
		ClientId: &clientToAppMgr.UUID{Value: strconv.Itoa(1)},
		GeoLocation: &clientToAppMgr.Location{
			Lat: 1.1,
			Lon: 1.1,
		},
		AppId: &clientToAppMgr.UUID{Value: strconv.Itoa(1)},
	})
	if err != nil {
		log.Println(err)
		return
	}
	taskList := list.GetTaskList()
	ips := [3]string{taskList[0].GetIp(), taskList[1].GetIp(), taskList[2].GetIp()}
	ports := [3]string{taskList[0].GetPort(), taskList[1].GetPort(), taskList[2].GetPort()}
	fmt.Printf("Servers notif : %v\n", ips)
	if ci.serverIPs[0] == "" {
		ci.serverIPs = ips
		ci.serverPorts = ports
		fmt.Printf("Servers if : %v\n", ips)
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
			ci.backupServers[ci.serverIPs[i]+":"+ci.serverPorts[i]] = true
		}
	}
	ci.mutexBestServer.Lock()
	if ci.taskIP != ips[0] && ci.taskPort != ports[0] {
		ci.taskIP = ips[0]
		ci.taskPort = ports[0]
		ci.newServer = true
	}
	ci.mutexBestServer.Unlock()
}

func (ci *ClientInfo) PeriodicFuncCalls(wg *sync.WaitGroup) {
	defer wg.Done()
	identifyBestServerTicker := time.NewTicker(5 * time.Second)
	queryListTicker := time.NewTicker(10 * time.Second)

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
	startIBS := time.Now()
	img := gocv.IMRead("data/test/Anthony_Fauci.jpg", gocv.IMReadColor)
	dims := img.Size()
	data := img.ToBytes()
	mattype := int32(img.Type())
	imgData := clientToTask.ImageData{
		Width:   int32(dims[0]),
		Height:  int32(dims[1]),
		MatType: mattype,
		Image:   data,
		ImageID: 123,
	}
	prevElapsedTime := time.Duration(0)
	taskIP := ci.taskIP
	taskPort := ci.taskPort
	for i := 0; i < nMultiConn; i++ {
		start := time.Now()
		for j := 0; j < 3; j++ {
			_, err := ci.service[ci.serverIPs[i]+":"+ci.serverPorts[i]].TestPerformance(context.Background(), &imgData)
			if err != nil {
				log.Fatalf("Error sending test image to %s:%v", ci.serverIPs[i]+":"+ci.serverPorts[i], err)
			}
		}
		elapsedTime := time.Since(start)
		if prevElapsedTime > elapsedTime {
			prevElapsedTime = elapsedTime
			taskIP = ci.serverIPs[i]
			taskPort = ci.serverPorts[i]
		}
	}
	ci.mutexBestServer.Lock()
	ci.taskIP = taskIP
	ci.taskPort = taskPort
	ci.newServer = true
	ci.mutexBestServer.Unlock()

	fmt.Printf("time taken %v\n", time.Since(startIBS))
	fmt.Println("*****************Identified servers********************")

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
			log.Fatalf("Client stide creation failed: %v", err)
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

	waitc := make(chan struct{})

	receiveData := make(map[int]string, 0)

	nImagesReceived := 0
	var counter *ratecounter.RateCounter
	start := time.Time{}
	go func() {

		// open display window
		window := gocv.NewWindow("Object Detect")
		defer window.Close()

		for {
			if _, ok := receiveData[nImagesReceived]; !ok {
				continue
			}

			stream := ci.stream[receiveData[nImagesReceived]]
			img, err := stream.Recv()
			if nImagesReceived == 0 {
				counter = ratecounter.NewRateCounter(1 * time.Second)
				// fmt.Printf("Frame latency - %v", time.Since(start))
			}
			if nImagesReceived == 20 {
				// counter = ratecounter.NewRateCounter(1 * time.Second)
				fmt.Printf("Frame latency - %v", time.Since(start))
			}
			// if err == io.EOF {
			// 	close(waitc)
			// 	return
			// }
			if err != nil {
				log.Fatalf("Image receive from app failed: %v", err)
			}
			ci.mutexMapAccess.Lock()
			delete(receiveData, nImagesReceived)
			ci.mutexMapAccess.Unlock()
			width := img.GetWidth()
			height := img.GetHeight()
			matType := img.GetMatType()
			data := img.GetImage()

			mat, err := gocv.NewMatFromBytes(int(width), int(height), gocv.MatType(matType), data)
			if err != nil {
				log.Fatalf("Error converting bytes to matrix: %v", err)
			}

			window.IMShow(mat)
			counter.Incr(1)
			if window.WaitKey(1) >= 0 {
				break
			}

			if _, ok := ci.backupServers[receiveData[nImagesReceived]]; ok {
				if ci.lastFrameLoc[receiveData[nImagesReceived]] == nImagesReceived {
					fmt.Printf("Deleted  %s entry\n", receiveData[nImagesReceived])
					ci.conns[receiveData[nImagesReceived]].Close()
					delete(ci.service, receiveData[nImagesReceived])
					delete(ci.stream, receiveData[nImagesReceived])
					delete(ci.conns, receiveData[nImagesReceived])
					delete(ci.lastFrameLoc, receiveData[nImagesReceived])

				}
			}
			nImagesReceived++
			fmt.Printf("%d\n", counter.Rate())
		}
	}()

	img := gocv.NewMat()
	defer img.Close()

	nImagesSent := 0
	for {
		// if ok := webcam.Read(&img); !ok {
		if ok := video.Read(&img); !ok {
			// fmt.Printf("Device closed: %v\n", deviceID)
			fmt.Printf("Video closed: %v\n", videoPath)
			break
		}
		if img.Empty() {
			continue
		}

		dims := img.Size()
		data := img.ToBytes()
		mattype := int32(img.Type())

		ci.mutexBestServer.Lock()
		if ci.newServer {
			taskIP = ci.taskIP
			taskPort = ci.taskPort
			ci.newServer = false
		}
		ci.mutexBestServer.Unlock()
		ci.mutexMapAccess.Lock()
		receiveData[nImagesSent] = taskIP + ":" + taskPort
		ci.mutexMapAccess.Unlock()
		ci.lastFrameLoc[taskIP+":"+taskPort] = nImagesSent
		// fmt.Printf("Received frame Map size %d\n", len(receiveData))
		// fmt.Printf("Stream Map size %d\n", len(ci.stream))
		// fmt.Printf("Stream address - %s\n", taskIP+":"+taskPort)
		stream := ci.stream[taskIP+":"+taskPort]
		if nImagesSent == 20 {
			start = time.Now()
		}

		t1 := time.Now()
		err = stream.Send(&clientToTask.ImageData{
			Width:   int32(dims[0]),
			Height:  int32(dims[1]),
			MatType: mattype,
			Image:   data,
		})

		fmt.Printf("Send latency - %v\n", time.Since(t1))
		if err != nil {
			log.Fatalf("Error sending image frame: %v", err)
		}

		nImagesSent++
	}
	<-waitc
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
