package main

import (
	"context"
	"errors"
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
	"google.golang.org/protobuf/types/known/durationpb"
)

type TaskServer struct {
	clientToTask.UnimplementedRpcClientToTaskServer

	IP             string
	ListenPort     string
	processingTime time.Duration
	mutexTime      *sync.Mutex
}

// performDetection analyzes the results from the detector network,
// which produces an output blob with a shape 1x1xNx7
// where N is the number of detections, and each detection
// is a vector of float values
// [batchId, classId, confidence, left, top, right, bottom]
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

func (ts *TaskServer) TestPerformance(ctx context.Context, testPerf *clientToTask.TestPerf) (*clientToTask.PerfData, error) {
	return &clientToTask.PerfData{
		ProcTime: durationpb.New(ts.processingTime),
	}, nil
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

func (ts *TaskServer) SendRecvImage(stream clientToTask.RpcClientToTask_SendRecvImageServer) error {

	model := "data/frozen_inference_graph.pb"
	config := "data/ssd_mobilenet_v1.pbtxt"
	backend := gocv.NetBackendDefault
	target := gocv.NetTargetCPU
	// open DNN object tracking model
	net := gocv.ReadNet(model, config)
	if net.Empty() {
		fmt.Printf("Error reading network model from : %v %v\n", model, config)
		return errors.New("Error reading network model")
	}
	defer net.Close()
	net.SetPreferableBackend(gocv.NetBackendType(backend))
	net.SetPreferableTarget(gocv.NetTargetType(target))

	ratio := 1.0 / 127.5
	mean := gocv.NewScalar(127.5, 127.5, 127.5, 0)
	swapRGB := true

	for {
		data := make([]byte, 3000000)
		var width int32
		var height int32
		var matType int32
		for {
			img, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				log.Fatalf("Image receive from app failed: %v", err)
			}
			// fmt.Println("Received chunk")

			if img.GetStart() == 1 {
				width = img.GetWidth()
				height = img.GetHeight()
				matType = img.GetMatType()
			}

			chunk := img.GetImage()
			data = append(data, chunk...)
			// fmt.Printf("mattype - %d\n", matType)
			if img.GetStart() == 0 {
				break
			}
		}

		t1 := time.Now()
		mat, err := gocv.NewMatFromBytes(int(width), int(height), gocv.MatType(matType), data)
		if err != nil {
			log.Fatalf("Error converting bytes to matrix: %v", err)
		}

		// convert image Mat to 300x300 blob that the object detector can analyze
		blob := gocv.BlobFromImage(mat, ratio, image.Pt(300, 300), mean, swapRGB, false)

		// feed the blob into the detector
		net.SetInput(blob, "")

		// run a forward pass thru the network
		prob := net.Forward("")

		performDetection(&mat, prob)

		prob.Close()
		blob.Close()

		ts.mutexTime.Lock()
		ts.processingTime = time.Since(t1)
		ts.mutexTime.Unlock()
		dims := mat.Size()
		imgdata := mat.ToBytes()
		mattype := int32(mat.Type())

		chunks := split(imgdata, 4096)
		nChunks := len(chunks)
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
	}
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
	ip := os.Args[1]
	listenPort := os.Args[2]

	ts := &TaskServer{IP: ip, ListenPort: listenPort}
	ts.ListenRoutine()
}
