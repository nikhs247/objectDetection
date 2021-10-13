package main

import (
	"log"
	"os"

	"github.com/nikhs247/objectDetection/client/objectdetectionclient"
)

func main() {

	if len(os.Args) != 5 {
		log.Println("Not enough parameters: [AM IP] [AM port] [location: which city] [tag]")
		return
	}
	appMgrIP := os.Args[1]
	appMgrPort := os.Args[2]
	loc := os.Args[3]
	tag := os.Args[4]

	objectdetectionclient.Run(appMgrIP, appMgrPort, loc, tag)
}
