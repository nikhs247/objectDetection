package objectdetectionclient

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nikhs247/objectDetection/comms/rpc/clientToTask"
)

func Run(appMgrIP string, appMgrPort string, where string, tag string, topN int) {

	// Capture the signal
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	// Initialize the client
	ci := Init(appMgrIP, appMgrPort, where, tag, topN)

	// Globle timestamp base point for print
	startTime := time.Now()

	// Probing nearby available edge servers and construct candidate lsit
	// After this step: we have the initial edge node candidate list and ready for processing
	for {
		err := ci.DiscoverAndProbing()
		if err != nil {
			fmt.Println("# Repeat DiscoverAndProbing() - error: " + err.Error())
		} else {
			break
		}
	}

	// Start an asynchronous routine to periodically update the candidate list
	go ci.PeriodicDiscoverAndProbing()

	// Start streaming
	go ci.Processing(startTime)

	select {
	// Wait for signal
	case <-signalChan:
		ci.mutexServerUpdate.Lock()
		currentService := ci.servers[ci.currentServer].service
		_, err := currentService.EndProcess(context.Background(), &clientToTask.EmptyMessage{})
		if err != nil {
			fmt.Println("^ " + err.Error())
		}
		fmt.Println("^ Manually close: notify current server of leaving")
		os.Exit(0)
	case <-ci.restart:
		return
	}
}
