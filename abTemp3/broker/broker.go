package main

import (
	"fmt"
	"io"
	"net"

	"github.com/ericnorway/arbitraryFailures/abTemp3/common"
	pb "github.com/ericnorway/arbitraryFailures/abTemp3/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

type Broker struct {
	pubChan      chan *pb.Publication
	subs         *Subscribers
	forwardedPub map[int64]map[int64]bool
}

func NewBroker() *Broker {
	return &Broker{
		pubChan:      make(chan *pb.Publication),
		subs:         NewSubscribers(),
		forwardedPub: make(map[int64]map[int64]bool),
	}
}

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
	sub := b.subs.addSubscriber(id, addr, subscribe.Topics)

	// Write loop
	go func() {
		for {
			select {
			case pub := <-sub.toSubCh:
				err := stream.Send(pub)
				if err != nil {
					b.subs.removeSubscriber(id)
					break
				}
			}
		}
	}()

	// Read loop
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			b.subs.removeSubscriber(id)
			return err
		} else if err != nil {
			b.subs.removeSubscriber(id)
			return err
		}
	}

	return nil
}

func (b Broker) handleMessages() {
	for {
		select {
		case req := <-b.pubChan:
			if req.PubType == common.AB {
				b.handleAbPublish(req)
			}
		}
	}
}

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
