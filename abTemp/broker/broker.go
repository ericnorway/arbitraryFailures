package main

import (
	"flag"
	"fmt"
	"net"
	"sync"
	
	pb "github.com/ericnorway/arbitraryFailures/abTemp/proto"
	"google.golang.org/grpc"
)

var (
	help = flag.Bool(
		"help",
		false,
		"Show usage help",
	)
	endpoint = flag.String(
		"endpoint",
		"",
		"The endpoint to listen on.",
	)
	brokerID = flag.Int(
		"id",
		0,
		"The ID for this broker.",		
	)
)

func usage() {
	flag.PrintDefaults()
}

// parseArgs() parses the command line arguments.
// The return argument indicates whether or not the function was successful.
func parseArgs() bool {
	flag.Usage = usage
	flag.Parse()
	if *help {
		flag.Usage()
		return false
	}

	if *endpoint == "" {
		fmt.Printf("Need to specify an endpoint.\n")
		return false
	}
	
	if *brokerID == 0 {
		fmt.Printf("Need to specify an ID.\n")
		return false
	}

	return true
}

type Broker struct {
	abFwdChansMutex sync.RWMutex
	abFwdChans map[string] chan *pb.AbFwdPublication
	forwardedPub map[int64] map[int64] bool
}

func NewBroker() *Broker {
	return &Broker{
		abFwdChans: make(map[string] chan *pb.AbFwdPublication),
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
	pb.RegisterAbPubBrokerServer(grpcServer, broker)
	pb.RegisterAbSubBrokerServer(grpcServer, broker)
	fmt.Printf("Preparing to serve incoming requests.\n")
	err = grpcServer.Serve(listener)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}
