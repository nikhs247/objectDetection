package main

import (
	"context"
	"io"
	"log"
	"os"
	"time"

	"github.com/nikhs247/objectDetection/comms/rpc/clientToTask"
	"gocv.io/x/gocv"
	"google.golang.org/grpc"
)

// Information required for each server candidate
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

	// Main loop
	for {
		// (1) Capture the frame at this iteration to be sent
		if ok := video.Read(&img); !ok {
			log.Printf("Video closed: %v\n", videoPath)
			break
		}
		if img.Empty() {
			continue
		}
		dims := img.Size()
		dataSend := img.ToBytes()
		mattype := int32(img.Type())

		// (2) Send the frame
		chunks := split(dataSend, 4096)
		nChunks := len(chunks)
		// Timer for frame latency
		t1 := time.Now()
		for i := 0; i < nChunks; i++ {
			// Send the header of this frame
			if i == 0 {
				err = ci.stream.Send(&clientToTask.ImageData{
					Start:    1, // header
					Width:    int32(dims[0]),
					Height:   int32(dims[1]),
					MatType:  mattype,
					Image:    chunks[i],
					ClientID: "[userid]",
				})
				if err != nil {
					log.Fatalf("connection terminated: %v", err)
				}
				// Send the last chunk
			} else if i == nChunks-1 {
				err = ci.stream.Send(&clientToTask.ImageData{
					Start:    0,
					Image:    chunks[i],
					ClientID: "[userid]",
				})
				if err != nil {
					log.Fatalf("connection terminated: %v", err)
				}
				// Send the regular chunk
			} else {
				err = ci.stream.Send(&clientToTask.ImageData{
					Start:    -1, // regular chunck
					Image:    chunks[i],
					ClientID: "[userid]",
				})
				if err != nil {
					log.Fatalf("connection terminated: %v", err)
				}
			}
		}

		// (3) Receive the result
		_, err := ci.stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("connection terminated: %v", err)
		}

		t2 := time.Now()

		log.Printf("Frame latency: %v\n", t2.Sub(t1))
	}
}

func main() {

	if len(os.Args) != 3 {
		log.Println("Need: [ip] [port]")
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
