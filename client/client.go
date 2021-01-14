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
	conns           map[string]*grpc.ClientConn
	service         map[string]clientToTask.RpcClientToCargoClient
	stream          map[string]clientToTask.RpcClientToCargo_SendRecvImageClient
	mutexBestServer *sync.Mutex
	taskIP          string
	taskPort        string
	newServer       bool
}

func Init() *ClientInfo {
	var ci ClientInfo
	ci.serverIPs = make([]string, nMultiConn)
	ci.serverPorts = make([]string, nMultiConn)
	ci.conns = make(map[string]*grpc.ClientConn, nMultiConn)
	ci.service = make(map[string]clientToTask.RpcClientToCargoClient, nMultiConn)
	ci.newServer = false

	return &ci
}

func (ci *ClientInfo) PeriodicFuncCalls() {
	uptimeTicker := time.NewTicker(5 * time.Second)
	// dateTicker := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-uptimeTicker.C:
			ci.IdentifyBestServer()
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
	go func() {
		counter := ratecounter.NewRateCounter(1 * time.Second)
		// open display window
		window := gocv.NewWindow("Object Detect")
		defer window.Close()

		for {
			img, err := stream.Recv()
			if err == io.EOF {
				close(waitc)
				return
			}
			if err != nil {
				log.Fatalf("Image receive from app failed: %v", err)
			}

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
		}
	}()

	img := gocv.NewMat()
	defer img.Close()
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
	}
	<-waitc
}

func main() {
	taskIP := os.Args[1]
	taskPort := os.Args[2]
	deviceID, err := strconv.Atoi(os.Args[3])
	if err != nil {
		log.Fatalf("Device ID conversion failed: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go StartStreaming(taskIP, taskPort, deviceID, &wg)
	wg.Wait()
}
