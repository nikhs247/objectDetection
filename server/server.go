package main

import (
	"fmt"
	"os"

	"github.com/nikhs247/objectDetection/server/objectdetectionserver"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Need 2 arguments: [ip] [port]")
		return
	}

	ip := os.Args[1]
	listenPort := os.Args[2]

	objectdetectionserver.Run(ip, listenPort)
}
