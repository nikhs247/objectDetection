package objectdetectionserver

import (
	"log"
	"sync"
	"time"

	"github.com/nikhs247/objectDetection/comms/rpc/clientToTask"
	"gocv.io/x/gocv"
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

func Run(ip, listenPort string) {
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
		mutexAlgo:  &sync.Mutex{},

		appInfo: ai,
		// real processing time
		updateTime:     time.Time{},
		processingTime: dur1,
		mutexProcTime:  &sync.Mutex{},
		// dummy processing time
		dummyUpdateTime:     time.Time{},
		dummyProcessingTime: dur2,
		mutexDummyProcTime:  &sync.Mutex{},
	}
	ts.ListenRoutine()
}
