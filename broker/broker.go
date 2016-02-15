package main

import (
	"fmt"
	"io"
	"net"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

// Broker is a struct containing channels used in communicating
// with read and write loops.
type Broker struct {
	pubChan      chan *pb.Publication
	subs         *Subscribers // A map of subscribers
	forwardedPub map[int64]map[int64]bool
}

// NewBroker returns a new Broker
func NewBroker() *Broker {
	return &Broker{
		pubChan:      make(chan *pb.Publication),
		subs:         NewSubscribers(),
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
			return err
		} else if err != nil {
			return err
		}

		// Add pub req for processing
		b.pubChan <- req
	}
	return nil
}

// Subscribe handles incoming Subscribe requests from subscribers
func (b *Broker) Subscribe(stream pb.SubBroker_SubscribeServer) error {

	// Read initial subscribe message
	subscribe, err := stream.Recv()
	if err == io.EOF {
		return err
	} else if err != nil {
		return err
	}

	pr, _ := peer.FromContext(stream.Context())
	addr := pr.Addr.String()
	id := subscribe.SubscriberID
	sub := b.subs.AddSubscriber(id, addr, subscribe.Topics)

	// Write loop
	go func() {
		for {
			select {
			case pub := <-sub.toSubCh:
				err := stream.Send(pub)
				if err != nil {
					b.subs.RemoveSubscriber(id)
					break
				}
			}
		}
	}()

	// Read loop
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			b.subs.RemoveSubscriber(id)
			return err
		} else if err != nil {
			b.subs.RemoveSubscriber(id)
			return err
		}
	}

	return nil
}

// handleMessages handles incoming messages
func (b Broker) handleMessages() {
	for {
		select {
		// If it's a publish request
		case req := <-b.pubChan:
			if req.PubType == common.AB {
				// Handle an Authenticated Broadcast publish request
				b.handleAbPublish(req)
			}
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
		b.subs.RLock()
		for _, sub := range b.subs.subs {
			// Only if they are interested in the topic
			if sub.topics[req.Topic] == true {
				sub.toSubCh <- req
			}
		}
		b.subs.RUnlock()

		// Mark this publication as sent
		b.forwardedPub[req.PublisherID][req.PublicationID] = true
	} else {
		fmt.Printf("Already forwarded publication %v by publisher %v\n", req.PublicationID, req.PublisherID)
	}
}
