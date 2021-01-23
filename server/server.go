package main

import (
	"context"
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

type ApplicationInfo struct {
	net     gocv.Net
	ratio   float64
	mean    gocv.Scalar
	swapRGB bool
}

type TaskServer struct {
	clientToTask.UnimplementedRpcClientToTaskServer

	IP             string
	ListenPort     string
	processingTime time.Duration
	mutexProcTime  *sync.Mutex
	appInfo        ApplicationInfo
	updateTime     time.Time
	mutexUpTime    *sync.Mutex
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
	ts.mutexUpTime.Lock()
	diff := time.Since(ts.updateTime)
	ts.mutexUpTime.Unlock()

	thresholdDuration, err := time.ParseDuration("1s")
	if err != nil {
		panic(err)
	}
	idle := false
	if diff > thresholdDuration {
		idle = true
	}
	// dur, err := time.ParseDuration("0h")
	// if err != nil {
	// 	panic(err)
	// }
	var procTime time.Duration
	ts.mutexProcTime.Lock()
	procTime = ts.processingTime
	ts.mutexProcTime.Unlock()
	if !idle {
		time.Sleep(procTime)
	}

	if idle {
		// fmt.Println("I am idle")
		t1 := time.Now()

		img := gocv.IMRead("data/dummyFrame.jpg", gocv.IMReadColor)

		dims := img.Size()
		width := dims[0]
		height := dims[1]
		data := img.ToBytes()
		matType := img.Type()

		mat, err := gocv.NewMatFromBytes(int(width), int(height), gocv.MatType(matType), data)
		if err != nil {
			log.Fatalf("Error converting bytes to matrix: %v", err)
		}

		// convert image Mat to 300x300 blob that the object detector can analyze
		blob := gocv.BlobFromImage(mat, ts.appInfo.ratio, image.Pt(300, 300), ts.appInfo.mean, ts.appInfo.swapRGB, false)

		// feed the blob into the detector
		ts.appInfo.net.SetInput(blob, "")

		// run a forward pass thru the network
		prob := ts.appInfo.net.Forward("")

		performDetection(&mat, prob)

		prob.Close()
		blob.Close()
		procTime = time.Since(t1)
	}
	return &clientToTask.PerfData{
		ProcTime: durationpb.New(procTime),
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

	for {
		data := make([]byte, 3000000)
		var width int32
		var height int32
		var matType int32
		ts.mutexUpTime.Lock()
		ts.updateTime = time.Now()
		ts.mutexUpTime.Unlock()
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
		blob := gocv.BlobFromImage(mat, ts.appInfo.ratio, image.Pt(300, 300), ts.appInfo.mean, ts.appInfo.swapRGB, false)

		// feed the blob into the detector
		ts.appInfo.net.SetInput(blob, "")

		// run a forward pass thru the network
		prob := ts.appInfo.net.Forward("")

		performDetection(&mat, prob)

		prob.Close()
		blob.Close()

		ts.mutexProcTime.Lock()
		ts.processingTime = time.Since(t1)
		// pTime := ts.processingTime
		ts.mutexProcTime.Unlock()
		// fmt.Printf("processing time - %v\n", pTime)
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

	dur, err := time.ParseDuration("0h")
	if err != nil {
		panic(err)
	}

	model := "data/frozen_inference_graph.pb"
	config := "data/ssd_mobilenet_v1.pbtxt"
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
		IP:             ip,
		ListenPort:     listenPort,
		mutexProcTime:  &sync.Mutex{},
		mutexUpTime:    &sync.Mutex{},
		processingTime: dur,
		appInfo:        ai,
	}
	ts.ListenRoutine()
}
