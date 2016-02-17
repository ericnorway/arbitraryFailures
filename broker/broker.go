package main

import (
	"fmt"
	"io"
	"net"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

// Broker is a struct containing channels used in communicating
// with read and write loops.
type Broker struct {
	publishers   *Publishers // A map of publishers
	subscribers  *Subscribers // A map of subscribers
	forwardedPub map[int64]map[int64]bool
}

// NewBroker returns a new Broker
func NewBroker() *Broker {
	return &Broker{
		publishers:   NewPublishers(),
		subscribers:  NewSubscribers(),
		forwardedPub: make(map[int64]map[int64]bool),
	}
}

// StartBroker creates and starts a new Broker.
// The Broker will listen on the endpoint provided.
func StartBroker(endpoint string) {
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

// Publish handles incoming Publish requests from publishers
func (b *Broker) Publish(stream pb.PubBroker_PublishServer) error {

	// Read initial publish message
	req, err := stream.Recv()
	if err == io.EOF {
		return err
	} else if err != nil {
		return err
	}

	// Create publisher client
	pr, _ := peer.FromContext(stream.Context())
	addr := pr.Addr.String()
	id := req.PublisherID
	/*publisher := */b.publishers.AddPublisherInfo(id, addr)
	
	// Add pub req for processing
	b.publishers.fromPublisherCh<- req

	// Write loop
	go func() {
		for {
			// Nothing to write yet
		}
	}()
	
	// Read loop.
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			b.publishers.RemovePublisherInfo(id)
			return err
		} else if err != nil {
			b.publishers.RemovePublisherInfo(id)
			return err
		}

		// Add pub req for processing
		b.publishers.fromPublisherCh<- req
	}
	return nil
}

// Echo handles incoming RBR echo requests from other brokers
func (b *Broker) Echo(context.Context, *pb.Publication) (*pb.EchoResponse, error){

	return &pb.EchoResponse{}, nil
}

// Ready handles incoming RBR ready requests from other brokers
func (b *Broker) Ready(context.Context, *pb.Publication) (*pb.ReadyResponse, error) {

	return &pb.ReadyResponse{}, nil
}

// Subscribe handles incoming Subscribe requests from subscribers
func (b *Broker) Subscribe(stream pb.SubBroker_SubscribeServer) error {

	// Read initial subscribe message
	req, err := stream.Recv()
	if err == io.EOF {
		return err
	} else if err != nil {
		return err
	}

	// Create subscriber client
	pr, _ := peer.FromContext(stream.Context())
	addr := pr.Addr.String()
	id := req.SubscriberID
	subscriber := b.subscribers.AddSubscriberInfo(id, addr, req.Topics)
	
	// The request is handled in AddSubscriberInfo()

	// Write loop
	go func() {
		for {
			select {
			case pub := <-subscriber.toSubscriberCh:
				err := stream.Send(pub)
				if err != nil {
					b.subscribers.RemoveSubscriberInfo(id)
					break
				}
			}
		}
	}()

	// Read loop
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			b.subscribers.RemoveSubscriberInfo(id)
			return err
		} else if err != nil {
			b.subscribers.RemoveSubscriberInfo(id)
			return err
		}
		b.subscribers.fromSubscriberCh<- req
	}

	return nil
}

// handleMessages handles incoming messages
func (b Broker) handleMessages() {
	for {
		select {
		// If it's a publish request
		case req := <-b.publishers.fromPublisherCh:
			if req.PubType == common.AB {
				// Handle an Authenticated Broadcast publish request
				b.handleAbPublish(req)
			}
		case req := <-b.subscribers.fromSubscriberCh:
			b.handleTopicChange(req)
		}
	}
}

// handleAbPublish handles Authenticated Broadcast publish requests.
// It takes the request as input.
func (b Broker) handleAbPublish(req *pb.Publication) {
	if b.forwardedPub[req.PublisherID] == nil {
		b.forwardedPub[req.PublisherID] = make(map[int64]bool)
	}

	// If this publication has not been forwared yet
	if b.forwardedPub[req.PublisherID][req.PublicationID] == false {
		req.BrokerID = int64(*brokerID)

		// Forward the publication to all subscribers
		b.subscribers.RLock()
		for _, subscriber := range b.subscribers.subscribers {
			// Only if they are interested in the topic
			if subscriber.topics[req.Topic] == true {
				subscriber.toSubscriberCh <- req
			}
		}
		b.subscribers.RUnlock()

		// Mark this publication as sent
		b.forwardedPub[req.PublisherID][req.PublicationID] = true
	} else {
		fmt.Printf("Already forwarded publication %v by publisher %v\n", req.PublicationID, req.PublisherID)
	}
}

func (b Broker) handleTopicChange(req *pb.SubRequest) {
	b.subscribers.ChangeTopics(req.SubscriberID, req.Topics)
}