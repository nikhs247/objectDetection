package objectdetectionserver

import (
	"context"
	"fmt"
	"image"
	"image/color"
	"io"
	"log"
	"net"
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

	IP         string
	ListenPort string
	// Real processing time: for users who probe the currently connected server
	updateTime     time.Time
	processingTime time.Duration
	mutexProcTime  *sync.Mutex
	// Dummy processing time: for users who probe the alternative servers
	dummyUpdateTime     time.Time
	dummyProcessingTime time.Duration
	mutexDummyProcTime  *sync.Mutex

	// Application specific
	appInfo   ApplicationInfo
	mutexAlgo *sync.Mutex
}

func (ts *TaskServer) TestPerformance(ctx context.Context, testPerf *clientToTask.TestPerf) (*clientToTask.PerfData, error) {
	clientID := testPerf.GetClientID()

	// First check the type of this probing request
	var diff time.Duration
	var procTime time.Duration
	// true = self check self, false = check others
	checkSelf := testPerf.Check
	// (1) user probing its own connected server: check real processing time
	if checkSelf {
		ts.mutexProcTime.Lock()
		procTime = ts.processingTime
		diff = time.Since(ts.updateTime)
		ts.mutexProcTime.Unlock()
		// self check self means the server must be already running: just get proctime and sleep
		time.Sleep(procTime)
		log.Printf("%s: SELD check SELF Processing time inside busy with diff %v -------- %v\n", clientID, diff, procTime)
		return &clientToTask.PerfData{
			ProcTime: durationpb.New(procTime),
		}, nil
	}

	// (2) user probing an alternative server check dummy processing time
	ts.mutexDummyProcTime.Lock()
	procTime = ts.dummyProcessingTime
	diff = time.Since(ts.dummyUpdateTime)
	ts.mutexDummyProcTime.Unlock()

	// threashold for false scenario
	thresholdDuration, err := time.ParseDuration("300ms")
	if err != nil {
		panic(err)
	}
	if diff < thresholdDuration {
		time.Sleep(procTime)
		log.Printf("%s: Processing time inside busy with diff %v---------------- %v\n", clientID, diff, procTime)
	} else {
		img := gocv.IMRead("dummydata/dummyFrame.jpg", gocv.IMReadColor)
		dims := img.Size()
		width := dims[0]
		height := dims[1]
		data := img.ToBytes()
		matType := img.Type()
		mat, err := gocv.NewMatFromBytes(int(width), int(height), gocv.MatType(matType), data)
		if err != nil {
			log.Fatalf("Error converting bytes to matrix: %v", err)
		}
		t1 := time.Now()
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
		procTime = time.Since(t1)

		ts.mutexDummyProcTime.Lock()
		ts.dummyUpdateTime = time.Now()
		ts.dummyProcessingTime = procTime
		ts.mutexDummyProcTime.Unlock()
		log.Printf("%s: Processing time inside idle with diff %v ---------------- %v\n", clientID, diff, procTime)
	}

	return &clientToTask.PerfData{
		ProcTime: durationpb.New(procTime),
	}, nil
}

func (ts *TaskServer) SendRecvImage(stream clientToTask.RpcClientToTask_SendRecvImageServer) error {

	for {
		data := make([]byte, 0)
		var clientID string

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

		// Decode the received frame
		mat, _ := gocv.IMDecode(data, -1)

		t1 := time.Now()
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

		t2 := time.Since(t1)

		// Update the real processing record
		ts.mutexProcTime.Lock()
		ts.updateTime = time.Now()
		ts.processingTime = t2
		pTime := ts.processingTime
		ts.mutexProcTime.Unlock()

		log.Printf("%s:Processing time - %v\n", clientID, pTime)

		// Send back the detection result
		err := stream.Send(&clientToTask.ProcessResult{
			ResponseCode: 1,
			Result:       "object detected",
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
