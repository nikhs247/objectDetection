module github.com/nikhs247/objectDetection/client

go 1.13

require (
	github.com/google/uuid v1.2.0
	github.com/nikhs247/objectDetection/client/objectdetectionclient v0.0.0-00010101000000-000000000000
	github.com/nikhs247/objectDetection/comms v0.0.0-20210512175859-b14f48536de9
	gocv.io/x/gocv v0.26.0
	google.golang.org/grpc v1.35.0
)

replace github.com/nikhs247/objectDetection/comms => /Users/lh/Desktop/go/src/github.com/nikhs247/objectDetection/comms

replace github.com/nikhs247/objectDetection/client/objectdetectionclient => /Users/lh/Desktop/go/src/github.com/nikhs247/objectDetection/client/objectdetectionclient
