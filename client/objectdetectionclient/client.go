package objectdetectionclient

import (
	"fmt"

	"github.com/nikhs247/objectDetection/comms/rpc/appcomm"
)

const nMultiConn = 3

func Run(appMgrIP string, appMgrPort string, where string, tag string) {

	loc := ConvertLocation(where)

	// Initialize the client
	ci := Init(appMgrIP, appMgrPort, loc, tag)

	// Probing nearby available edge servers and construct candidate lsit
	// After this step: we have the initial edge node candidate list and ready for processing
	ci.DiscoverAndProbing()

	// Start an asynchronous routine to periodically update the candidate list
	go ci.PeriodicDiscoverAndProbing()

	// Start streaming
	ci.Processing()

	fmt.Println("Processing done!")
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
