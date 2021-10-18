package objectdetectionserver

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/nikhs247/objectDetection/comms/rpc/clientToTask"
	"gocv.io/x/gocv"
	"google.golang.org/protobuf/types/known/durationpb"
)

func (ts *TaskServer) RTT_Request(ctx context.Context, testPerf *clientToTask.TestPerf) (*clientToTask.PerfData, error) {
	return &clientToTask.PerfData{
		ProcTime: nil,
	}, nil
}

func (ts *TaskServer) Probe_Request(ctx context.Context, testPerf *clientToTask.TestPerf) (*clientToTask.ProbeResult, error) {
	// First check if this is the self-probe
	ts.mutexProcTime.Lock()
	val, ok := ts.clients[testPerf.ClientID]
	ts.mutexProcTime.Unlock()
	if ok {
		thresholdDuration, err := time.ParseDuration("300ms")
		if err != nil {
			panic(err)
		}
		// If this client is currently processing on this node: just return its current processing time
		if time.Since(val.updateTime) < thresholdDuration {
			return &clientToTask.ProbeResult{
				StateNumber: -1, // -1 represents self-check: StateNumber is irrelavent
				WhatIfTime:  durationpb.New(val.processingTime),
			}, nil
		}
	}
	// Check the "what-if" time
	ts.mutexState.Lock()
	stateNumber := ts.stateNumber
	whatIfTime := ts.whatIfTime
	ts.mutexState.Unlock()
	return &clientToTask.ProbeResult{
		StateNumber: stateNumber,
		WhatIfTime:  durationpb.New(whatIfTime),
	}, nil
}

func (ts *TaskServer) Join_Request(ctx context.Context, decision *clientToTask.Decision) (*clientToTask.JoinResult, error) {
	ts.mutexState.Lock()
	// Allow the client to join if this is an initial request or
	// The state hasn't changed since the client's last probe
	if decision.LastSate == ts.stateNumber {
		// After this client actually joins, invoke dummy workload to predict what-if processing time
		fmt.Println("Join_Request accepted --> invoke dummy workload")
		go ts.PerformDummyWorkloadWithDelay(300)
		return &clientToTask.JoinResult{
			Success: true,
		}, nil
	} else {
		ts.mutexState.Unlock()
		return &clientToTask.JoinResult{
			Success: false,
		}, nil
	}
}

func (ts *TaskServer) UnexpectedClientJoin(ctx context.Context, emptyMessage *clientToTask.EmptyMessage) (*clientToTask.EmptyMessage, error) {
	// This is invoked by client when an edge node fails
	// Return this call ASAP since the clinet waits there to continue its service after the faliure switch
	// Set a delay here to make sure that this server starts to serve the unexpected client
	fmt.Println("UnexpectedClientJoin --> invoke dummy workload")
	go ts.PerformDummyWorkload(200, true)
	return &clientToTask.EmptyMessage{}, nil
}

func (ts *TaskServer) EndProcess(ctx context.Context, emptyMessage *clientToTask.EmptyMessage) (*clientToTask.EmptyMessage, error) {
	// This is invoked by client when a better node is found and it switches to that node
	// Return this call ASAP since the clinet waits there to continue its service after the switch decision
	// Set a delay here to make sure that this client actually leaves
	fmt.Println("EndProcess --> invoke dummy workload")
	go ts.PerformDummyWorkload(200, false)
	return &clientToTask.EmptyMessage{}, nil
}

func (ts *TaskServer) SendRecvImage(stream clientToTask.RpcClientToTask_SendRecvImageServer) error {

	var clientID string
	for {
		data := make([]byte, 0)

		// Read one frame
		for {
			img, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				log.Printf("Connection closed by client")
				return nil
			}

			if img.GetStart() == 1 {
				clientID = img.GetClientID()
			}

			chunk := img.GetImage()
			data = append(data, chunk...)

			if img.GetStart() == 0 {
				break
			}
		}

		// Decode the received frame
		mat, _ := gocv.IMDecode(data, -1)

		t1 := time.Now()
		ts.PerformFrame(mat)
		procTime := time.Since(t1)

		// TUpdate the clients map, update time and processing time into the client record
		ts.mutexProcTime.Lock()
		ts.clients[clientID] = ClientState{
			updateTime:     time.Now(),
			processingTime: procTime,
		}
		ts.mutexProcTime.Unlock()

		log.Printf("%s:Processing time - %v\n", clientID, procTime)

		// Send back the detection result
		err := stream.Send(&clientToTask.ProcessResult{
			ResponseCode: 1,
			Result:       "object detected",
		})
		if err != nil {
			log.Printf("Connection closed by client")
			return nil
		}
	}
}
