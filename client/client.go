package main

import (
	"log"
	"os"
	"strconv"

	"github.com/nikhs247/objectDetection/client/objectdetectionclient"
)

func main() {

	if len(os.Args) != 6 {
		log.Println("Not enough parameters: [AM IP] [AM port] [location: which city] [tag] [TopN]")
		return
	}
	appMgrIP := os.Args[1]
	appMgrPort := os.Args[2]
	loc := os.Args[3]
	tag := os.Args[4]

	topN, err := strconv.Atoi(os.Args[5])
	if err != nil {
		log.Println("Wrong input format: TopN should be number")
		return
	}

	for {
		objectdetectionclient.Run(appMgrIP, appMgrPort, loc, tag, topN)
	}
}
