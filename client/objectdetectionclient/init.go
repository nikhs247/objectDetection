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

	// Application manager info
	appManagerConn    *grpc.ClientConn // keep this pointer so that we can close it at the end
	appManagerService appcomm.ApplicationManagerClient

	// Shared data structure
	// Current selected server and 3 server slots
	currentServer int // index of servers
	servers       []*ServerConnection

	// Lock for shared data structure
	mutexServerUpdate *sync.Mutex
}

func Init(appMgrIP string, appMgrPort string, where *appcomm.Location, tag string) *ClientInfo {

	// (1) Set up client info
	clientId := guuid.New().String()

	// which application this client is requesting for
	whichApp := &appcomm.UUID{Value: strconv.Itoa(1)}

	// (2) Build up the connection to Application Manager
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	appConn, err := grpc.Dial(appMgrIP+":"+appMgrPort, opts...)
	if err != nil {
		log.Println("Fail to connect appManager at the beginning")
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
		// Application Manager info
		appManagerConn:    appConn,
		appManagerService: appService,
		// Initialize candidate list info
		currentServer: -1,
		// Lock
		mutexServerUpdate: &sync.Mutex{},
	}
	return ci
}
