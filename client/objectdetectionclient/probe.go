package objectdetectionclient

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/nikhs247/objectDetection/comms/rpc/clientToTask"
	"google.golang.org/grpc"
)

type ServerConnection struct {
	ip      string
	port    string
	conn    *grpc.ClientConn
	service clientToTask.RpcClientToTaskClient
	stream  clientToTask.RpcClientToTask_SendRecvImageClient
	state   int64
}

func (ci *ClientInfo) DiscoverAndProbing() error {
	// (1) Query available edge nodes from Application Manager
	taskList := ci.queryAppManager()
	if len(taskList) == 0 {
		return errors.New("no available edge node [This error won't happen now, since query fails if 0 available in appManager]")
	}

	// (2) Get currently-using server and candidate list
	ci.mutexServerUpdate.Lock()
	currentServer_tmp := ci.currentServer
	servers_tmp := ci.servers
	ci.mutexServerUpdate.Unlock()

	// (3) Construct the candidate list for main thread
	var newServers []*ServerConnection
	var garbageList []*ServerConnection
	var err error
	if currentServer_tmp == -1 {
		// This is the initial probing call
		newServers, garbageList, err = ci.getCandidateListInit(taskList)
		if err != nil {
			return err
		}
	} else {
		// This is the periodic probing call
		newServers, garbageList, err = ci.getCandidateList(taskList, currentServer_tmp, servers_tmp)
		if err != nil {
			return err
		}
	}

	// (4) Update the candidate list for main thread
	ci.mutexServerUpdate.Lock()
	ci.servers = newServers
	ci.currentServer = 0
	ci.mutexServerUpdate.Unlock()

	// (5) Clean up the garbage pool
	go cleanUp(garbageList)
	return nil
}

func (ci *ClientInfo) PeriodicDiscoverAndProbing() {
	rand.Seed(time.Now().UTC().UnixNano())
	queryListTicker := time.NewTicker(5*time.Second + time.Duration(rand.Float32()*2)*(time.Second))
	for {
		select {
		// Periodic query is [5 - 7] seconds
		case <-queryListTicker.C:
			for {
				err := ci.DiscoverAndProbing()
				if err != nil {
					fmt.Println("# Repeat DiscoverAndProbing() - error: " + err.Error())
				} else {
					break
				}
			}
			rand.Seed(time.Now().UTC().UnixNano())
			queryListTicker = time.NewTicker(5*time.Second + time.Duration(rand.Float32()*2)*(time.Second))
		// Close this routine upon failure
		case <-ci.stopProbing:
			return
		}
	}
}
