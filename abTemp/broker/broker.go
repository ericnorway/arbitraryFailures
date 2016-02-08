package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	
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

/*type Subscriber struct {
	Conn grpc.ClientConn
	Stream pb.PubBrokerAB_PublishClient
}

type Subscribers struct {
	sync.Mutex
	Subscibers
}*/

type Broker struct {
	forwardChan chan pb.AbPubRequest
	forwardedPub map[int64] map[int64] bool
}

func NewBroker() *Broker {
	return &Broker{
		forwardChan: make(chan pb.AbPubRequest, 32),
		forwardedPub: make(map[int64] map[int64] bool),
	}
}

func (r *Broker) Publish(stream pb.AbPubBroker_PublishServer) error {
	fmt.Printf("Started Publish().\n")
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			fmt.Printf("EOF\n")
			stream.Send(&pb.AbErrorMsg{
				Message: "Closed",
			})
			return err
		} else if err != nil {
			fmt.Printf("%v\n", err)
			stream.Send(&pb.AbErrorMsg{
				Message: fmt.Sprintf("%v\n", err),
			})
			return err
		}
		r.forwardChan <- *req
		fmt.Printf("Publish received. %v\n", req)
	}
	return nil
}

func (r *Broker) Subscribe(stream pb.AbSubBroker_SubscribeServer) error {
	fmt.Printf("Started Subscribe().\n")
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			fmt.Printf("EOF\n")
			stream.Send(&pb.AbFwdPublication{})
			return err
		} else if err != nil {
			fmt.Printf("%v\n", err)
			stream.Send(&pb.AbFwdPublication{})
			return err
		}
		//r.forwardChan <- *req
		fmt.Printf("Subscribe received.\n")
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
	
	go broker.forwarding()
	
	grpcServer := grpc.NewServer()
	pb.RegisterAbPubBrokerServer(grpcServer, broker)
	pb.RegisterAbSubBrokerServer(grpcServer, broker)
	fmt.Printf("Preparing to serve incoming requests.\n")
	err = grpcServer.Serve(listener)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func (b *Broker) forwarding() {
	fmt.Printf("Started forwarding().\n")

	/*grpcOpts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithTimeout(50 * time.Millisecond),
		grpc.WithInsecure(),
	}
	dialOpts := pb.WithGrpcDialOptions(grpcOpts...)

	mgr, err := pb.NewManager(
		[]string{"localhost:11115", "localhost:11116"},
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
	}*/
	
	for {
		select {
			case pubReq := <-b.forwardChan:
				if b.forwardedPub[pubReq.PublisherID] == nil {
					b.forwardedPub[pubReq.PublisherID] = make(map[int64] bool)
				}
				if b.forwardedPub[pubReq.PublisherID][pubReq.PublicationID] == false {
					/*forwardConfig.FwdPublication(&pb.FwdRequest{
						PublisherID: pubReq.PublisherID,
						PublicationID: pubReq.PublicationID,
						BrokerID: int64(*brokerID),
						Publication: pubReq.Publication,
					})*/
					fmt.Printf("DEBUG: Forwarded\n")
					b.forwardedPub[pubReq.PublisherID][pubReq.PublicationID] = true
				}
			default:
		}
	}
}
