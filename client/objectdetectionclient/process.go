package objectdetectionclient

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nikhs247/objectDetection/comms/rpc/clientToTask"
	"gocv.io/x/gocv"
)

func (ci *ClientInfo) Processing(startTime time.Time) {
	// Set up video source [camera or video file]
	videoPath := "video/vid.avi"
	video, err := gocv.VideoCaptureFile(videoPath)
	if err != nil {
		log.Printf("Error opening video capture file: %s\n", videoPath)
		os.Exit(0)
	}
	defer video.Close()
	img := gocv.NewMat()
	defer img.Close()

	// Main loop for client: send frames out to server and get results
	// Server may fail: there are 3 Send() and 1 Recv() functions could lead to error during data transfer
	// Call faultTolerance() to handle connection switch

	// stream is the local variable for currently selected server
	var stream clientToTask.RpcClientToTask_SendRecvImageClient

Loop:
	for {
		// (1) Capture the frame at this iteration to be sent
		if ok := video.Read(&img); !ok {
			// To extend the experiment time: if the video hits end, we re-open the video
			video, err = gocv.VideoCaptureFile(videoPath)
			if err != nil {
				log.Printf("Error opening video capture file: %s\n", videoPath)
				os.Exit(0)
			}
			continue
		}
		if img.Empty() {
			continue
		}

		// (2) Get the server for processing this frame
		ci.mutexServerUpdate.Lock()
		whichIp := ci.servers[ci.currentServer].ip // this is used for emulated data
		stream = ci.servers[ci.currentServer].stream
		ci.mutexServerUpdate.Unlock()

		// (3) Send the frame
		// Encode this frame
		dataSend, _ := gocv.IMEncode(".jpg", img)
		split(dataSend, 4096)
		chunks := split(dataSend, 4096)
		nChunks := len(chunks)

		// Timer for frame latency
		t1 := time.Now()

		for i := 0; i < nChunks; i++ {
			// Send the header of this frame
			if i == 0 {
				err = stream.Send(&clientToTask.ImageData{
					Start:    1, // header
					Image:    chunks[i],
					ClientID: ci.id,
				})
				if err != nil {
					if ci.faultTolerance() {
						continue Loop
					}
					return
				}
				// Send the last chunk
			} else if i == nChunks-1 {
				err = stream.Send(&clientToTask.ImageData{
					Start:    0,
					Image:    chunks[i],
					ClientID: ci.id,
				})
				if err != nil {
					if ci.faultTolerance() {
						continue Loop
					}
					return
				}
				// Send the regular chunk
			} else {
				err = stream.Send(&clientToTask.ImageData{
					Start:    -1, // regular chunck
					Image:    chunks[i],
					ClientID: ci.id,
				})
				if err != nil {
					if ci.faultTolerance() {
						continue Loop
					}
					return
				}
			}
		}

		// (4) Receive the result
		_, err := stream.Recv()
		if err != nil {
			if ci.faultTolerance() {
				continue Loop
			}
			return
		}

		t2 := time.Now()
		elapsedFromStart := time.Since(startTime)

		// Print out the frame latency
		fmt.Printf("* %s %v %v \n", whichIp, elapsedFromStart, t2.Sub(t1))
	}
}

func (ci *ClientInfo) faultTolerance() bool {
	ci.mutexServerUpdate.Lock()
	if ci.currentServer+1 == len(ci.servers) {
		ci.mutexServerUpdate.Unlock()
		// Note: This will rarely happen if we add more duplicated connections
		fmt.Println("$ All candidates failed: no available servers and abort")

		// Stop the probing routine
		ci.stopProbing <- 1
		// Stop the main routine
		ci.restart <- 1
		// Stop the processing routine
		return false
	}
	ci.currentServer++
	nextService := ci.servers[ci.currentServer].service
	ci.mutexServerUpdate.Unlock()

	// Notify the next server that I am coming
	nextService.UnexpectedClientJoin(context.Background(), &clientToTask.EmptyMessage{})

	fmt.Println("! Server just failed: switch to a backup server!!")
	return true
}

func split(buf []byte, lim int) [][]byte {
	var chunk []byte
	chunks := make([][]byte, 0, len(buf)/lim+1)
	for len(buf) >= lim {
		chunk, buf = buf[:lim], buf[lim:]
		chunks = append(chunks, chunk)
	}
	if len(buf) > 0 {
		chunks = append(chunks, buf[:])
	}
	return chunks
}
