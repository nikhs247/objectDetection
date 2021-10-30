package objectdetectionclient

import (
	"log"
	"os"
	"strconv"
	"sync"

	guuid "github.com/google/uuid"
	"github.com/nikhs247/objectDetection/comms/rpc/appcomm"
	"google.golang.org/grpc"
)

type ClientInfo struct {
	// Clinet info
	id       string
	tag      string // used to specify LAN resources
	location *appcomm.Location
	appId    *appcomm.UUID
	topN     int

	// Application manager info
	appManagerConn    *grpc.ClientConn // keep this pointer so that we can close it at the end
	appManagerService appcomm.ApplicationManagerClient

	// Shared data structure
	// Current selected server and 3 server slots
	currentServer int // index of servers
	servers       []*ServerConnection

	// Lock for shared data structure
	mutexServerUpdate *sync.Mutex

	// Message channels for client restart
	stopProbing chan int
	restart     chan int
}

func Init(appMgrIP string, appMgrPort string, whereStr string, tag string, n int) *ClientInfo {

	// (1) Set up client info
	clientId := guuid.New().String()

	// which application this client is requesting for
	whichApp := &appcomm.UUID{Value: strconv.Itoa(1)}

	where := ConvertLocation(whereStr)

	// (2) Build up the connection to Application Manager
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	appConn, err := grpc.Dial(appMgrIP+":"+appMgrPort, opts...)
	if err != nil {
		log.Println("^ Fail to connect appManager at the beginning")
		os.Exit(0)
	}
	appService := appcomm.NewApplicationManagerClient(appConn)

	// (3) Construct the ClientInfo
	ci := &ClientInfo{
		// Client info
		id:       clientId,
		tag:      tag,
		location: where,
		appId:    whichApp,
		topN:     n,
		// Application Manager info
		appManagerConn:    appConn,
		appManagerService: appService,
		// Initialize candidate list info
		currentServer: -1,
		// Lock
		mutexServerUpdate: &sync.Mutex{},
		// Message channels for restart
		stopProbing: make(chan int),
		restart:     make(chan int),
	}
	return ci
}

func ConvertLocation(where string) *appcomm.Location {
	// Hardcode for converting geo-location for now
	var loc *appcomm.Location
	if where == "Minneapolis" { // Minneapolis: close
		loc = &appcomm.Location{
			Lat: 44.98,
			Lon: -93.24,
		}
	} else if where == "Duluth" { // Duluth: far
		loc = &appcomm.Location{
			Lat: 46.79,
			Lon: -92.11,
		}
	} else {
		loc = &appcomm.Location{ // Rochester: other
			Lat: 44.02,
			Lon: -92.47,
		}
	}
	return loc
}
