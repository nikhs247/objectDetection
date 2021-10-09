package main

import (
	"os"

	"github.com/nikhs247/objectDetection/server/objectdetectionserver"
)

func main() {
	ip := os.Args[1]
	listenPort := os.Args[2]

	objectdetectionserver.Run(ip, listenPort)
}
