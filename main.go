package main

import (
	"image"
	"image/color"
	"log"
	"time"

	"gocv.io/x/gocv"
)

func main() {

	// // Capture video from the camera
	// deviceID := 0
	// webcam, err := gocv.OpenVideoCapture(deviceID)
	// if err != nil {
	// 	fmt.Printf("Error opening video capture device: %v\n", deviceID)
	// 	return
	// }
	// defer webcam.Close()

	// Capture the video from the video file
	videoPath := "data/vid.avi"
	video, err := gocv.VideoCaptureFile(videoPath)
	if err != nil {
		log.Printf("Error opening video capture file: %s\n", videoPath)
		return
	}
	defer video.Close()

	// Open DNN object tracking model and required parameters
	model := "model/frozen_inference_graph.pb"
	config := "model/ssd_mobilenet_v1.pbtxt"
	net := gocv.ReadNet(model, config)
	if net.Empty() {
		log.Printf("Error reading network model from : %v %v\n", model, config)
		return
	}
	defer net.Close()

	backend := gocv.NetBackendDefault
	target := gocv.NetTargetCPU
	net.SetPreferableBackend(gocv.NetBackendType(backend))
	net.SetPreferableTarget(gocv.NetTargetType(target))

	ratio := 1.0 / 127.5
	mean := gocv.NewScalar(127.5, 127.5, 127.5, 0)
	swapRGB := true

	// // Set up video on screen and video rate calculation
	// window := gocv.NewWindow("DNN Detection")
	// defer window.Close()

	// Main loop: process one frame at one iteration
	img := gocv.NewMat()
	defer img.Close()
	for {
		// // Capture one frame from camera
		// if ok := webcam.Read(&img); !ok {
		// 	log.Printf("Device closed: %v\n", deviceID)
		// 	return
		// }
		// if img.Empty() {
		// 	continue
		// }

		// Capture one frame from the video file
		if ok := video.Read(&img); !ok {
			log.Printf("Video closed: %v\n", videoPath)
			break
		}
		if img.Empty() {
			continue
		}

		t1 := time.Now()
		// convert image Mat to 300x300 blob that the object detector can analyze
		blob := gocv.BlobFromImage(img, ratio, image.Pt(300, 300), mean, swapRGB, false)

		// feed the blob into the detector
		net.SetInput(blob, "")

		// run a forward pass thru the network
		prob := net.Forward("")

		performDetection(&img, prob)

		prob.Close()
		blob.Close()
		t2 := time.Now()
		log.Printf("Frame processing time: %v \n", t2.Sub(t1))

		// window.IMShow(img)
		// if window.WaitKey(1) >= 0 {
		// 	break
		// }
	}
}

// performDetection analyzes the results from the detector network,
// which produces an output blob with a shape 1x1xNx7
// where N is the number of detections, and each detection
// is a vector of float values
// [batchId, classId, confidence, left, top, right, bottom]
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
