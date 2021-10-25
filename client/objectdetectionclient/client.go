package objectdetectionclient

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/nikhs247/objectDetection/comms/rpc/clientToTask"
)

func Run(appMgrIP string, appMgrPort string, where string, tag string, topN int) {

	// Capture the signal
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	// Initialize the client
	ci := Init(appMgrIP, appMgrPort, where, tag, topN)

	// Probing nearby available edge servers and construct candidate lsit
	// After this step: we have the initial edge node candidate list and ready for processing
	for {
		err := ci.DiscoverAndProbing()
		if err != nil {
			fmt.Println(err.Error())
		} else {
			break
		}
		fmt.Println("bbbb")
	}

	// Start an asynchronous routine to periodically update the candidate list
	go ci.PeriodicDiscoverAndProbing()

	// Start streaming
	go ci.Processing()

	// Wait for signal
	<-signalChan
	ci.mutexServerUpdate.Lock()
	currentService := ci.servers[ci.currentServer].service
	_, err := currentService.EndProcess(context.Background(), &clientToTask.EmptyMessage{})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Manually close: notify current server of leaving")
}
