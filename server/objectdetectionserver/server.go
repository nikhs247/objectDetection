package objectdetectionserver

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/nikhs247/objectDetection/comms/rpc/clientToTask"
	"gocv.io/x/gocv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type TaskServer struct {
	clientToTask.UnimplementedRpcClientToTaskServer
	// Server config
	IP         string
	ListenPort string

	// Server state mutex: how many users currently connected to this node
	mutexState  *sync.Mutex
	stateNumber int64
	whatIfTime  time.Duration
	numOfUsers  int

	// Application specific
	appInfo   ApplicationInfo
	mutexAlgo *sync.Mutex

	// Dummy workload
	dummyWorkload gocv.Mat

	// Real processing time: for users who probe the currently-using server
	clients       map[string]ClientState
	mutexProcTime *sync.Mutex
}

type ClientState struct {
	updateTime     time.Time
	processingTime time.Duration
}

func Run(ip, listenPort string) {

	// Prepare the application inference model (object detection)
	model := "model/frozen_inference_graph.pb"
	config := "model/ssd_mobilenet_v1.pbtxt"
	gonet := gocv.ReadNet(model, config)
	if gonet.Empty() {
		log.Fatalf("Error reading network model from : %v %v\n", model, config)
	}
	defer gonet.Close()

	backend := gocv.NetBackendDefault
	target := gocv.NetTargetCPU
	gonet.SetPreferableBackend(gocv.NetBackendType(backend))
	gonet.SetPreferableTarget(gocv.NetTargetType(target))

	app := ApplicationInfo{
		gonet:   gonet,
		ratio:   1.0 / 127.5,
		mean:    gocv.NewScalar(127.5, 127.5, 127.5, 0),
		swapRGB: true,
	}

	// Construct TaskServer data structure
	ts := &TaskServer{
		IP:         ip,
		ListenPort: listenPort,

		mutexState:  &sync.Mutex{},
		stateNumber: 0,
		numOfUsers:  0,

		appInfo:   app,
		mutexAlgo: &sync.Mutex{},

		dummyWorkload: ConfigureDummyWorkload(),

		// real processing time
		clients:       make(map[string]ClientState),
		mutexProcTime: &sync.Mutex{},
	}

	// Invoke the dummy workload to get the initial "what-if time": average of 2 invocations
	t1 := time.Now()
	ts.PerformFrame(ts.dummyWorkload)
	t2 := time.Now()
	ts.PerformFrame(ts.dummyWorkload)
	t3 := time.Now()
	ts.whatIfTime = (t2.Sub(t1) + t3.Sub(t2)) / 2
	log.Printf("Profiling processing time: %v\n", ts.whatIfTime)

	// Start the gRPC server
	listen, err := net.Listen("tcp", fmt.Sprintf("%s:%s", ts.IP, ts.ListenPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	clientToTask.RegisterRpcClientToTaskServer(grpcServer, ts)
	reflection.Register(grpcServer)
	grpcServer.Serve(listen)
}
