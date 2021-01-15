.PHONY: genProto

genProto:
	protoc -I=comms/protos --go_out=plugins=grpc:comms/rpc/clientToTask --go_opt=paths=source_relative clientToTask.proto
	protoc -I=comms/protos --go_out=plugins=grpc:comms/rpc/clientToAppMgr --go_opt=paths=source_relative clientToAppMgr.proto
	protoc -I=comms/protos --go_out=plugins=grpc:comms/rpc/appcomm --go_opt=paths=source_relative appcomm.proto