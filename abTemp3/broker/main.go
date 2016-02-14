package main

import (
	"fmt"
	"net"
	"sync"
	
	pb "github.com/ericnorway/arbitraryFailures/abTemp3/proto"
	"google.golang.org/grpc"
)

type Broker struct {
	pubChan chan *pb.Publication
	subChansMutex sync.RWMutex
	subChans map[string] chan *pb.Publication
	forwardedPub map[int64] map[int64] bool
}

func NewBroker() *Broker {
	return &Broker{
		pubChan: make(chan *pb.Publication),
		subChans: make(map[string] chan *pb.Publication),
		forwardedPub: make(map[int64] map[int64] bool),
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
	pb.RegisterPubBrokerServer(grpcServer, broker)
	pb.RegisterSubBrokerServer(grpcServer, broker)
	go broker.handleMessages()
	
	fmt.Printf("Preparing to serve incoming requests.\n")
	err = grpcServer.Serve(listener)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}
