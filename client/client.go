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
	"github.com/paulbellamy/ratecounter"
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
	ci.lastFrameLoc = make(map[string]int, nMultiConn)
	ci.conns = make(map[string]*grpc.ClientConn, nMultiConn)
	ci.service = make(map[string]clientToTask.RpcClientToTaskClient, nMultiConn)
	ci.stream = make(map[string]clientToTask.RpcClientToTask_SendRecvImageClient, nMultiConn)
	ci.frameTimer = make(map[int]time.Time, nMultiConn)
	ci.mutexBestServer = &sync.Mutex{}
	ci.mutexTimer = &sync.Mutex{}
	ci.mutexServerUpdate = &sync.Mutex{}
	ci.newServer = false

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	// opts = append(opts, grpc.WithBlock())
	// fmt.Println("Dialing to app mgr " + appMgrIP + ":" + appMgrPort)
	conn, err := grpc.Dial(appMgrIP+":"+appMgrPort, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	// fmt.Println("Completed connection to app mgr")
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
	fmt.Printf("Servers notif ips : %v\n", ips)
	fmt.Printf("Servers notif ports: %v\n", ports)
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
			ci.mutexServerUpdate.Lock()
			ci.backupServers[ci.serverIPs[i]+":"+ci.serverPorts[i]] = true
			ci.mutexServerUpdate.Unlock()
		}
	}

	ci.mutexServerUpdate.Lock()
	for i := 0; i < nMultiConn; i++ {
		ci.serverIPs[i] = ips[i]
		ci.serverPorts[i] = ports[i]
	}
	ci.mutexServerUpdate.Unlock()

	// setup conn, service and stream for new server if one exists in the query list
	for i := 0; i < nMultiConn; i++ {
		if _, ok := ci.conns[ips[i]+":"+ports[i]]; !ok {
			ci.mutexServerUpdate.Lock()
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
			ci.mutexServerUpdate.Unlock()
		}
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
		ImageID: 123,
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
		// fmt.Printf("P - %v --- E - %v\n", prevElapsedTime, elapsedTime)
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
	// fmt.Printf("servers - %s:%s\n", taskIP, taskPort)
	ci.mutexBestServer.Unlock()

	// fmt.Printf("time taken %v\n", time.Since(startIBS))
	// fmt.Println("*****************Identified servers********************")

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
		// fmt.Println(key)
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
		// fmt.Printf("%v\n", ci.stream[key])
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
	// start := time.Time{}
	go func() {

		// open display window
		// window := gocv.NewWindow("Object Detect")
		// defer window.Close()
		counter = ratecounter.NewRateCounter(1 * time.Second)

		for {
			ci.mutexServerUpdate.Lock()
			if _, ok := receiveData[nImagesReceived]; !ok {
				ci.mutexServerUpdate.Unlock()
				continue
			}
			stream := ci.stream[receiveData[nImagesReceived]]
			ci.mutexServerUpdate.Unlock()
			data := make([]byte, 0)
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

				if img.GetMatType() != 123 && img.GetMatType() != -1 {
					width = img.GetWidth()
					height = img.GetHeight()
					matType = img.GetMatType()
				}

				chunk := img.GetImage()
				data = append(data, chunk...)
				if img.GetMatType() == -1 {
					break
				}
			}
			ci.mutexServerUpdate.Lock()
			delete(receiveData, nImagesReceived)
			ci.mutexServerUpdate.Unlock()

			ci.mutexTimer.Lock()
			// fmt.Printf("Frame latency: %v\n", time.Since(ci.frameTimer[nImagesReceived]))
			ci.mutexTimer.Unlock()

			// mat, err := gocv.NewMatFromBytes(int(width), int(height), gocv.MatType(matType), data)
			_, err := gocv.NewMatFromBytes(int(width), int(height), gocv.MatType(matType), data)
			if err != nil {
				log.Fatalf("Error converting bytes to matrix: %v", err)
			}

			// window.IMShow(mat)
			counter.Incr(1)
			// if window.WaitKey(1) >= 0 {
			// 	break
			// }

			ci.mutexServerUpdate.Lock()
			if _, ok := ci.backupServers[receiveData[nImagesReceived]]; ok {
				if ci.lastFrameLoc[receiveData[nImagesReceived]] == nImagesReceived {
					// fmt.Printf("Deleted  %s entry\n", receiveData[nImagesReceived])
					ci.conns[receiveData[nImagesReceived]].Close()
					delete(ci.service, receiveData[nImagesReceived])
					delete(ci.stream, receiveData[nImagesReceived])
					delete(ci.conns, receiveData[nImagesReceived])
					delete(ci.lastFrameLoc, receiveData[nImagesReceived])

				}
			}
			ci.mutexServerUpdate.Unlock()
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

		// fmt.Println("Next frame")
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
		ci.mutexServerUpdate.Lock()
		receiveData[nImagesSent] = taskIP + ":" + taskPort
		ci.lastFrameLoc[taskIP+":"+taskPort] = nImagesSent
		stream := ci.stream[taskIP+":"+taskPort]
		ci.mutexServerUpdate.Unlock()

		chunks := split(data, 1024)
		nChunks := len(chunks)

		ci.mutexTimer.Lock()
		ci.frameTimer[nImagesSent] = time.Now()
		ci.mutexTimer.Unlock()

		// fmt.Printf("Number of chunks %d\n", nChunks)
		for i := 0; i < nChunks; i++ {
			if i == 0 {
				err = stream.Send(&clientToTask.ImageData{
					Width:   int32(dims[0]),
					Height:  int32(dims[1]),
					MatType: mattype,
					Image:   chunks[i],
				})

				if err != nil {
					log.Fatalf("Error sending image frame: %v", err)
				}
				// fmt.Println("Send chunk 0")
			} else if i == nChunks-1 {
				err = stream.Send(&clientToTask.ImageData{
					MatType: -1,
					Image:   chunks[i],
				})

				if err != nil {
					log.Fatalf("Error sending image frame: %v", err)
				}

				// fmt.Printf("Send chunk %d\n", nChunks-1)

			} else {
				err = stream.Send(&clientToTask.ImageData{
					MatType: 123,
					Image:   chunks[i],
				})

				if err != nil {
					log.Fatalf("Error sending image frame: %v", err)
				}
			}
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
