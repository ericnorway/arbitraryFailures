package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"time"
	
	pb "github.com/ericnorway/arbitraryFailures/temp/proto"
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
	ForwardChan chan pb.PubRequest
}

func NewBroker() *Broker {
	return &Broker{
		ForwardChan: make(chan pb.PubRequest, 32),
	}
}

func (r *Broker) Publish(stream pb.PublisherAC_PublishServer) error {
	fmt.Printf("Started Publish().\n")
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			fmt.Printf("EOF\n")
			return stream.SendAndClose(&pb.ErrorMsg{
				Message: "Closed",
			})
		} else if err != nil {
			fmt.Printf("%v\n", err)
			return stream.SendAndClose(&pb.ErrorMsg{
				Message: fmt.Sprintf("%v\n", err),
			})
		}
		r.ForwardChan <- *req
		fmt.Printf("Publish received.\n")
	}
	return nil
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
	
	go broker.Forwarding()
	
	grpcServer := grpc.NewServer()
	pb.RegisterPublisherACServer(grpcServer, broker)
	fmt.Printf("Preparing to serve incoming requests.\n")
	err = grpcServer.Serve(listener)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func (r *Broker) Forwarding() {
	fmt.Printf("Started Forwarding().\n")

	grpcOpts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithTimeout(50 * time.Millisecond),
		grpc.WithInsecure(),
	}
	dialOpts := pb.WithGrpcDialOptions(grpcOpts...)

	mgr, err := pb.NewManager(
		[]string{"localhost:11114", "localhost:11115"},
		dialOpts,
	)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
	defer mgr.Close()

	// Get all all available machine ids
	ids := mgr.MachineIDs()

	// A forward configuration
	forwardConfig, err := mgr.NewConfiguration(ids, 2, time.Second)
	if err != nil {
		fmt.Printf("error creating read config: %v\n", err)
	}
	
	for {
		select {
			case pubReq := <-r.ForwardChan:
				forwardConfig.Forward(&pb.FwdRequest{
					PublisherID: pubReq.PublisherID,
					PublicationID: pubReq.PublicationID,
					BrokerID: int64(*brokerID),
					Publication: pubReq.Publication,
				})
			default:
		}
	}
}
