package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/nikhs247/objectDetection/comms/rpc/clientToTask"
	"gocv.io/x/gocv"
	"google.golang.org/grpc"
)

type ServerConnection struct {
	ip      string
	port    string
	conn    *grpc.ClientConn
	service clientToTask.RpcClientToTaskClient
	stream  clientToTask.RpcClientToTask_SendRecvImageClient
}

func (ci *ServerConnection) StartStreaming() {
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

	for {
		if ok := video.Read(&img); !ok {
			fmt.Printf("Video closed: %v\n", videoPath)
			break
		}
		if img.Empty() {
			continue
		}

		// Encode this frame
		dataSend, _ := gocv.IMEncode(".jpg", img)
		chunks := split(dataSend, 4096)
		nChunks := len(chunks)
		stream := ci.stream

		t1 := time.Now()

		for i := 0; i < nChunks; i++ {
			// Send the header of this frame
			if i == 0 {
				err = stream.Send(&clientToTask.ImageData{
					Start:    1, // header
					Image:    chunks[i],
					ClientID: "c1",
				})
				if err != nil {
					log.Fatalf("connection terminated: %v", err)
				}
				// Send the last chunk
			} else if i == nChunks-1 {
				err = stream.Send(&clientToTask.ImageData{
					Start:    0,
					Image:    chunks[i],
					ClientID: "c1",
				})
				if err != nil {
					log.Fatalf("connection terminated: %v", err)
				}
				// Send the regular chunk
			} else {
				err = stream.Send(&clientToTask.ImageData{
					Start:    -1, // regular chunck
					Image:    chunks[i],
					ClientID: "c1",
				})
				if err != nil {
					log.Fatalf("connection terminated: %v", err)
				}
			}
		}

		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("connection terminated: %v", err)
		}

		t2 := time.Now()
		fmt.Printf("Frame latency: %v\n", t2.Sub(t1))
	}
}

func main() {

	if len(os.Args) != 3 {
		fmt.Println("Need: [ip] [port]")
		return
	}
	ip := os.Args[1]
	port := os.Args[2]

	serverConn, err := grpc.Dial(ip+":"+port, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Connection to server failed: %v", err)
	}
	serverService := clientToTask.NewRpcClientToTaskClient(serverConn)
	serverStream, err := serverService.SendRecvImage(context.Background())
	if err != nil {
		log.Fatalf("Connection servers failed: %v", err)
	}
	ci := &ServerConnection{
		ip:      ip,
		port:    port,
		conn:    serverConn,
		service: serverService,
		stream:  serverStream,
	}

	// Main thread
	ci.StartStreaming()

	log.Println("Processing done!")
}

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
