package objectdetectionserver

import (
	"image"
	"image/color"
	"log"
	"time"

	"gocv.io/x/gocv"
)

type ApplicationInfo struct {
	gonet   gocv.Net
	ratio   float64
	mean    gocv.Scalar
	swapRGB bool
}

func ConfigureDummyWorkload() gocv.Mat {
	img := gocv.IMRead("dummydata/dummyFrame.jpg", gocv.IMReadColor)
	dims := img.Size()
	width := dims[0]
	height := dims[1]
	data := img.ToBytes()
	matType := img.Type()
	mat, err := gocv.NewMatFromBytes(int(width), int(height), gocv.MatType(matType), data)
	if err != nil {
		log.Fatalf("Error converting bytes to matrix: %v", err)
	}
	return mat
}

func (ts *TaskServer) PerformDummyWorkloadWithDelay(delay time.Duration) {
	// Wait for the client to actually start processing
	time.Sleep(delay * time.Millisecond)

	// Increment state number
	ts.stateNumber += 1

	// One more user joined
	ts.numOfUsers += 1

	// Invoke the dummy workload
	t1 := time.Now()
	ts.PerformFrame(ts.dummyWorkload)
	whatIfTime := time.Since(t1)

	// Update the what-if processing time
	ts.whatIfTime = whatIfTime

	// After invoking dummy workload, release the lock for server state!
	ts.mutexState.Unlock()
}

func (ts *TaskServer) PerformDummyWorkload(delay time.Duration, joinOrLeave bool) {
	ts.mutexState.Lock()
	// Wait for the client to actually start processing
	time.Sleep(delay * time.Millisecond)

	// Increment state number
	ts.stateNumber += 1

	// Modify the number_of_users record
	if joinOrLeave {
		ts.numOfUsers += 1
	} else {
		ts.numOfUsers -= 1
	}

	// Invoke the dummy workload
	t1 := time.Now()
	ts.PerformFrame(ts.dummyWorkload)
	whatIfTime := time.Since(t1)

	// Update the what-if processing time
	ts.whatIfTime = whatIfTime
	ts.mutexState.Unlock()
}

func (ts *TaskServer) PerformFrame(mat gocv.Mat) {
	blob := gocv.BlobFromImage(mat, ts.appInfo.ratio, image.Pt(300, 300), ts.appInfo.mean, ts.appInfo.swapRGB, false)
	ts.mutexAlgo.Lock()
	// feed the blob into the detector
	ts.appInfo.gonet.SetInput(blob, "")
	// run a forward pass thru the network
	prob := ts.appInfo.gonet.Forward("")
	ts.mutexAlgo.Unlock()
	performDetection(&mat, prob)
	prob.Close()
	blob.Close()
}

func performDetection(frame *gocv.Mat, results gocv.Mat) {
	for i := 0; i < results.Total(); i += 7 {
		confidence := results.GetFloatAt(0, i+2)
		if confidence > 0.5 {
			left := int(results.GetFloatAt(0, i+3) * float32(frame.Cols()))
			top := int(results.GetFloatAt(0, i+4) * float32(frame.Rows()))
			right := int(results.GetFloatAt(0, i+5) * float32(frame.Cols()))
			bottom := int(results.GetFloatAt(0, i+6) * float32(frame.Rows()))
			gocv.Rectangle(frame, image.Rect(left, top, right, bottom), color.RGBA{0, 255, 0, 0}, 2)
		}
	}
}
