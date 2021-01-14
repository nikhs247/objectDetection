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

func StartStreaming(taskIP string, taskPort string, deviceID int, wg *sync.WaitGroup) {
	defer wg.Done()
	conn, err := grpc.Dial(taskIP+":"+taskPort, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Connection to server failed: %v", err)
	}
	defer conn.Close()

	service := clientToTask.NewRpcClientToCargoClient(conn)

	// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// defer cancel()

	stream, err := service.SendRecvImage(context.Background())
	if err != nil {
		log.Fatalf("Client stide creation failed: %v", err)
	}

	// open capture device
	// webcam, err := gocv.OpenVideoCapture(deviceID)
	// if err != nil {
	// 	fmt.Printf("Error opening video capture device: %v\n", deviceID)
	// 	return
	// }
	// defer webcam.Close()

	// open video to capture
	videoPath := "client/video/vid.avi"
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
