module github.com/nikhs247/objectDetection/server/objectdetectionserver

go 1.13

replace github.com/nikhs247/objectDetection/comms => ../../comms

require (
	github.com/nikhs247/objectDetection/comms v0.0.0-20211016180600-23007913d1c2
	gocv.io/x/gocv v0.26.0
	google.golang.org/grpc v1.35.0
	google.golang.org/protobuf v1.25.0
)
