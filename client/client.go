package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/nikhs247/objectDetection/comms/rpc/clientToTask"
	"github.com/paulbellamy/ratecounter"
	"gocv.io/x/gocv"
	"google.golang.org/grpc"
)

const nMultiConn = 3

type ClientInfo struct {
	appManagerIP    string
	appManagerPort  string
	serverIPs       []string
	serverPorts     []string
	backupServers   map[string]bool
	lastFrameLoc    map[string]int
	conns           map[string]*grpc.ClientConn
	service         map[string]clientToTask.RpcClientToCargoClient
	stream          map[string]clientToTask.RpcClientToCargo_SendRecvImageClient
	mutexBestServer *sync.Mutex
	taskIP          string
	taskPort        string
	newServer       bool
}

func Init(appMgrIP string, appMgrPort string) *ClientInfo {
	var ci ClientInfo
	ci.appManagerIP = appMgrIP
	ci.appManagerPort = appMgrPort
	ci.serverIPs = make([]string, nMultiConn)
	ci.serverPorts = make([]string, nMultiConn)
	ci.backupServers = make(map[string]bool, nMultiConn)
	ci.lastFrameLoc = make(map[string]int, nMultiConn)
	ci.conns = make(map[string]*grpc.ClientConn, nMultiConn)
	ci.service = make(map[string]clientToTask.RpcClientToCargoClient, nMultiConn)
	ci.stream = make(map[string]clientToTask.RpcClientToCargo_SendRecvImageClient, nMultiConn)
	ci.newServer = false

	return &ci
}

func (ci *ClientInfo) QueryListFromAppManager() {
	ips := []string{"1", "2", "3"}
	ports := []string{"1", "2", "3"}
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
	var taskIP string
	var taskPort string
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
		ci.service[key] = clientToTask.NewRpcClientToCargoClient(conn)
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

	var receiveData map[int]string
	nImagesReceived := 0
	go func() {
		counter := ratecounter.NewRateCounter(1 * time.Second)
		// open display window
		window := gocv.NewWindow("Object Detect")
		defer window.Close()

		for {
			if _, ok := receiveData[nImagesReceived]; !ok {
				continue
			}
			stream := ci.stream[receiveData[nImagesReceived]]
			img, err := stream.Recv()
			// if err == io.EOF {
			// 	close(waitc)
			// 	return
			// }
			if err != nil {
				log.Fatalf("Image receive from app failed: %v", err)
			}
			delete(receiveData, nImagesReceived)
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
			fmt.Printf("%d\n", counter.Rate())
			if _, ok := ci.backupServers[receiveData[nImagesReceived]]; ok {
				if ci.lastFrameLoc[receiveData[nImagesReceived]] == nImagesReceived {
					ci.conns[receiveData[nImagesReceived]].Close()
					delete(ci.service, receiveData[nImagesReceived])
					delete(ci.stream, receiveData[nImagesReceived])
					delete(ci.conns, receiveData[nImagesReceived])
					delete(ci.lastFrameLoc, receiveData[nImagesReceived])
				}
			}
			nImagesReceived++
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
		receiveData[nImagesSent] = taskIP + ":" + taskPort
		ci.lastFrameLoc[taskIP+":"+taskPort] = nImagesSent
		stream := ci.stream[taskIP+":"+taskPort]
		err = stream.Send(&clientToTask.ImageData{
			Width:   int32(dims[0]),
			Height:  int32(dims[1]),
			MatType: mattype,
			Image:   data,
		})
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
	var wg sync.WaitGroup
	wg.Add(1)
	go ci.StartStreaming(&wg)
	wg.Add(1)
	go ci.PeriodicFuncCalls(&wg)
	wg.Wait()
}
