package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	
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
		fmt.Printf("Need to specify an endpoint\n")
		return false
	}

	return true
}

type Subscriber struct {
	forwardChan chan pb.FwdRequest

	// The first index references the publisher ID.
	// The second index references the publication ID.
	// The third index references the broker ID.
	// The byte slice contains the publication.
	pubsReceived map[int64] map[int64] map[int64] []byte
	
	// The first index references the publisher ID.
	// The second index references the publication ID.
	// The byte slice contains the publication.
	pubsLearned map[int64] map[int64] []byte
}

func NewSubscriber() *Subscriber {
	return &Subscriber{
		forwardChan: make(chan pb.FwdRequest, 32),
		pubsReceived: make(map[int64] map[int64] map[int64] []byte),
		pubsLearned: make(map[int64] map[int64] []byte),
	}
}

func main() {
	parsedCorrectly := parseArgs()
	if parsedCorrectly {
		startSubscriber(*endpoint)
	}
}

func startSubscriber(endpoint string) {
	fmt.Printf("Subscriber started.\n")

	listener, err := net.Listen("tcp", endpoint)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("Listener started on %v\n", endpoint)
	}
	
	subscriber := NewSubscriber()
	go subscriber.processPublications()
	
	grpcServer := grpc.NewServer()
	pb.RegisterBrokerACServer(grpcServer, subscriber)
	fmt.Printf("Preparing to serve incoming requests.\n")
	err = grpcServer.Serve(listener)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func (s *Subscriber) Forward(stream pb.BrokerAC_ForwardServer) error {
	fmt.Printf("Started Forward().\n")
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
		
		s.forwardChan <- *req
	}
	return nil
}

func (s *Subscriber) processPublications() {
	fmt.Printf("Started processPublications().\n")

	for {
		select {
			case pub := <-s.forwardChan:
				// Make the map so not trying to access nil reference
				if s.pubsReceived[pub.PublisherID] == nil {
					s.pubsReceived[pub.PublisherID] = make(map[int64] map[int64] []byte)
				}
				// Make the map so not trying to access nil reference
				if s.pubsReceived[pub.PublisherID][pub.PublicationID] == nil {
					s.pubsReceived[pub.PublisherID][pub.PublicationID] = make(map[int64] []byte)
				}
				// Publication has not been received yet for this publisher ID, publication ID, broker ID
				if s.pubsReceived[pub.PublisherID][pub.PublicationID][pub.BrokerID] == nil {
					// So record it
					s.pubsReceived[pub.PublisherID][pub.PublicationID][pub.BrokerID] = pub.Publication
					// Check if there is a quorum yet for this publisher ID and publication ID
					s.checkQuorum(pub.PublisherID, pub.PublicationID, 3)
				}
			default:
		}
	}
}

func (s *Subscriber) checkQuorum(publisherID int64, publicationID int64, quorumSize int) bool {
	// It's nil, so nothing to check.
	if s.pubsReceived[publisherID] == nil {
		s.pubsReceived[publisherID] = make(map[int64] map[int64] []byte)
		return false
	}
	// It's nil, so nothing to check.
	if s.pubsReceived[publisherID][publicationID] == nil {
		s.pubsReceived[publisherID][publicationID] = make(map[int64] []byte)
		return false
	}
	
	// Make the map so not trying to access nil reference
	if s.pubsLearned[publisherID] == nil {
		s.pubsLearned[publisherID] = make(map[int64] []byte)
	}
	// If already learned this publication
	if s.pubsLearned[publisherID][publicationID] != nil {
		fmt.Printf("Already learned publication %v from publisher %v.\n", publicationID, publisherID)
		return false
	}
	
	// Just a temporary map to help with checking for a quorum. It keeps track of the number of each
	// publication value with this publisher ID and publication ID.
	countMap := make(map[string] int)
	
	for _, publication := range s.pubsReceived[publisherID][publicationID] {
		pub := string(publication)
		countMap[pub] = countMap[pub] + 1
		if countMap[pub] >= quorumSize {
			s.pubsLearned[publisherID][publicationID] = publication
			fmt.Printf("Learned publication %v from publisher %v.\n", publicationID, publisherID)
			return true
		}
	}
	
	return false
}