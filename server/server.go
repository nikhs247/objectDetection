package main

import (
	"fmt"
	"image"
	"image/color"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/nikhs247/objectDetection/comms/rpc/clientToTask"
	"gocv.io/x/gocv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type ApplicationInfo struct {
	net     gocv.Net
	ratio   float64
	mean    gocv.Scalar
	swapRGB bool
}

type TaskServer struct {
	clientToTask.UnimplementedRpcClientToTaskServer

	IP         string
	ListenPort string
	mutexAlgo  *sync.Mutex
	appInfo    ApplicationInfo
}

func (ts *TaskServer) SendRecvImage(stream clientToTask.RpcClientToTask_SendRecvImageServer) error {
	log.Println("One new user conencted")
	for {
		data := make([]byte, 0)
		var width int32
		var height int32
		var matType int32
		var clientID string

		// (1) Read one frame in chunks
		for {
			img, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				log.Printf("Connection closed by client")
				return nil
			}

			if img.GetStart() == 1 {
				width = img.GetWidth()
				height = img.GetHeight()
				matType = img.GetMatType()
				clientID = img.GetClientID()
			}

			chunk := img.GetImage()
			data = append(data, chunk...)

			if img.GetStart() == 0 {
				break
			}
		}

		// (2) Start processing the frame
		t1 := time.Now()
		mat, err := gocv.NewMatFromBytes(int(width), int(height), gocv.MatType(matType), data)
		if err != nil {
			log.Fatalf("Error converting bytes to matrix: %v", err)
		}

		// convert image Mat to 300x300 blob that the object detector can analyze
		blob := gocv.BlobFromImage(mat, ts.appInfo.ratio, image.Pt(300, 300), ts.appInfo.mean, ts.appInfo.swapRGB, false)

		ts.mutexAlgo.Lock()
		// feed the blob into the detector
		ts.appInfo.net.SetInput(blob, "")

		// run a forward pass thru the network
		prob := ts.appInfo.net.Forward("")
		ts.mutexAlgo.Unlock()

		performDetection(&mat, prob)

		prob.Close()
		blob.Close()

		t2 := time.Now()

		log.Printf("%s Processing time: %v\n", clientID, t2.Sub(t1))

		// Send the processing result back
		err = stream.Send(&clientToTask.ProcessResult{
			ResponseCode: 1,
			Result:       "object detected",
		})
		if err != nil {
			log.Printf("Connection closed by client")
			return nil
		}
	}
	log.Println("One new user terminated")
	return nil
}

func (ts *TaskServer) ListenRoutine() {
	listen, err := net.Listen("tcp", fmt.Sprintf("%s:%s", ts.IP, ts.ListenPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	clientToTask.RegisterRpcClientToTaskServer(grpcServer, ts)
	reflection.Register(grpcServer)
	grpcServer.Serve(listen)
}

func main() {
	if len(os.Args) != 2 {
		log.Println(" Need port number")
		return
	}
	ip := "0.0.0.0"
	listenPort := os.Args[1]

	model := "model/frozen_inference_graph.pb"
	config := "model/ssd_mobilenet_v1.pbtxt"
	net := gocv.ReadNet(model, config)
	if net.Empty() {
		log.Fatalf("Error reading network model from : %v %v\n", model, config)
	}
	defer net.Close()

	backend := gocv.NetBackendDefault
	target := gocv.NetTargetCPU
	net.SetPreferableBackend(gocv.NetBackendType(backend))
	net.SetPreferableTarget(gocv.NetTargetType(target))

	ai := ApplicationInfo{
		net:     net,
		ratio:   1.0 / 127.5,
		mean:    gocv.NewScalar(127.5, 127.5, 127.5, 0),
		swapRGB: true,
	}
	ts := &TaskServer{
		IP:         ip,
		ListenPort: listenPort,
		mutexAlgo:  &sync.Mutex{},
		appInfo:    ai,
	}
	log.Println("Server start running...")
	ts.ListenRoutine()
}

func performDetection(frame *gocv.Mat, results gocv.Mat) {
	for i := 0; i < results.Total(); i += 7 {
		confidence := results.GetFloatAt(0, i+2)
		if confidence > 0.5 {
			left := int(results.GetFloatAt(0, i+3) * float32(frame.Cols()))
			top := int(results.GetFloatAt(0, i+4) * float32(frame.Rows()))
			right := int(results.GetFloatAt(0, i+5) * float32(frame.Cols()))
			bottom := int(results.GetFloatAt(0, i+6) * float32(frame.Rows()))
			gocv.Rectangle(frame, image.Rect(left, top, right, bottom), color.RGBA{0, 255, 0, 0}, 2)
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
