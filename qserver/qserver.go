package main

import (
	"fmt"
	"image"
	"image/color"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nikhs247/objectDetection/comms/rpc/clientToTask"
	"gocv.io/x/gocv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// This is the server listensing user requests
type TaskServer struct {
	clientToTask.UnimplementedRpcClientToTaskServer

	IP         string
	ListenPort string
	// Pointer to the Request queue
	queue chan *Req
	// Real processing time: for users who probe the currently connected server
	updateTime     time.Time
	processingTime time.Duration
	mutexProcTime  *sync.Mutex
	// Dummy processing time: for users who probe the alternative servers
	dummyUpdateTime     time.Time
	dummyProcessingTime time.Duration
	mutexDummyProcTime  *sync.Mutex
}

type Req struct {
	data       []byte
	respBuffer chan string
}

func (ts *TaskServer) SendRecvImage(stream clientToTask.RpcClientToTask_SendRecvImageServer) error {
	log.Println("One new user conencted")
	for {
		data := make([]byte, 0)
		var clientID string

		// Read frame
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
				clientID = img.GetClientID()
			}

			chunk := img.GetImage()
			data = append(data, chunk...)

			if img.GetStart() == 0 {
				break
			}
		}

		t1 := time.Now()

		// Push the pointer of the data and requst into the queue
		resultBuffer := make(chan string)
		ts.queue <- &Req{
			data:       data,
			respBuffer: resultBuffer,
		}

		// Waiting for the processing result
		result := <-resultBuffer

		t2 := time.Since(t1)

		// Update the real processing record
		ts.mutexProcTime.Lock()
		ts.updateTime = time.Now()
		ts.processingTime = t2
		pTime := ts.processingTime
		ts.mutexProcTime.Unlock()

		log.Printf("%s:Queuing delay + Processing time - %v\n", clientID, pTime)

		// Send back the detection result
		err := stream.Send(&clientToTask.ProcessResult{
			ResponseCode: 1,
			Result:       result,
		})
		if err != nil {
			log.Printf("Connection closed by client")
			return nil
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

	// ========================== prepare the application model ==================
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

	// ========================== initiate the request queue =======================
	queue := make(chan *Req, 10)

	// ============================ start the server ===============================
	dur1, err := time.ParseDuration("0h")
	if err != nil {
		panic(err)
	}
	dur2, err := time.ParseDuration("0h")
	if err != nil {
		panic(err)
	}
	ts := &TaskServer{
		IP:         ip,
		ListenPort: listenPort,
		// pointer to the request channel
		queue: queue,
		// real processing time
		updateTime:     time.Time{},
		processingTime: dur1,
		mutexProcTime:  &sync.Mutex{},
		// dummy processing time
		dummyUpdateTime:     time.Time{},
		dummyProcessingTime: dur2,
		mutexDummyProcTime:  &sync.Mutex{},
	}
	go ts.ListenRoutine()
	fmt.Println("Server now listening ...")

	// ========================= main thread for processing the request ===============
	// Set to accept os signal channel
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case req := <-queue:
			// process this request
			// decode the received frame
			mat, _ := gocv.IMDecode(req.data, -1)

			// convert image Mat to 300x300 blob that the object detector can analyze
			ratio := 1.0 / 127.5
			mean := gocv.NewScalar(127.5, 127.5, 127.5, 0)
			swapRGB := true
			blob := gocv.BlobFromImage(mat, ratio, image.Pt(300, 300), mean, swapRGB, false)

			// feed the blob into the detector
			net.SetInput(blob, "")

			// run a forward pass thru the network
			prob := net.Forward("")

			performDetection(&mat, prob)

			prob.Close()
			blob.Close()

			// send back the result
			req.respBuffer <- "Object detected"
		case <-signalChan:
			fmt.Println("Self shuting down")
			return
		}
	}
}

// Note: the queue won't increase rapidly if there are few users.
// New request will only be sent to the server when got the previous response

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
