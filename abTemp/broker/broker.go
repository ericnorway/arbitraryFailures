package main

import (
	"fmt"
	"net"
	"sync"
	
	pb "github.com/ericnorway/arbitraryFailures/abTemp/proto"
	"google.golang.org/grpc"
)

type Broker struct {
	abPubChan chan *pb.AbPubRequest
	abFwdChansMutex sync.RWMutex
	abFwdChans map[string] chan *pb.AbFwdPublication
	abForwardedPub map[int64] map[int64] bool
}

func NewBroker() *Broker {
	return &Broker{
		abPubChan: make(chan *pb.AbPubRequest),
		abFwdChans: make(map[string] chan *pb.AbFwdPublication),
		abForwardedPub: make(map[int64] map[int64] bool),
	}
}

func main() {
	parsedCorrectly := parseArgs()
	if parsedCorrectly {
		startBroker(*endpoint)
	}
}

func startBroker(endpoint string) {
	fmt.Printf("Broker started.\n")

	listener, err := net.Listen("tcp", endpoint)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("Listener started on %v\n", endpoint)
	}
	
	broker := NewBroker()
	
	grpcServer := grpc.NewServer()
	pb.RegisterAbPubBrokerServer(grpcServer, broker)
	pb.RegisterAbSubBrokerServer(grpcServer, broker)
	go broker.processAbMessages()
	
	fmt.Printf("Preparing to serve incoming requests.\n")
	err = grpcServer.Serve(listener)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}
