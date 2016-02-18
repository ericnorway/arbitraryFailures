package main

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	// "google.golang.org/grpc/peer"
)

// Broker is a struct containing channels used in communicating
// with read and write loops.
type Broker struct {
	// PUBLISHER CHANNEL VARIABLES
	fromPublisherCh chan *pb.Publication

	// BROKER CHANNEL VARIABLES
	toBrokerEchoChs   *BrokerEchoChannels
	fromBrokerEchoCh  chan *pb.Publication
	toBrokerReadyChs  *BrokerReadyChannels
	fromBrokerReadyCh chan *pb.Publication

	// SUBSCRIBER CHANNEL VARIABLES
	toSubscriberChs  *SubscriberPubChannels
	fromSubscriberCh chan *pb.SubRequest

	// MESSAGE TRACKING VARIABLES

	// The first index references the publisher ID.
	// The second index references the publication ID.
	// The bool contains whether or not it was sent yet.
	forwardSent map[int64]map[int64]bool

	// The first index references the publisher ID.
	// The second index references the publication ID.
	// The bool contains whether or not it was sent yet.
	echoesSent map[int64]map[int64]bool

	// The first index references the publisher ID.
	// The second index references the publication ID.
	// The third index references the broker ID.
	// The byte slice contains the publication.
	echoesReceived map[int64]map[int64]map[int64][]byte

	// The first index references the publisher ID.
	// The second index references the publication ID.
	// The bool contains whether or not it was sent yet.
	readiesSent map[int64]map[int64]bool

	// The first index references the publisher ID.
	// The second index references the publication ID.
	// The third index references the broker ID.
	// The byte slice contains the publication.
	readiesReceived map[int64]map[int64]map[int64][]byte

	// The first index references the subscriber ID.
	// The second index references the topic ID.
	// The bool contains whether or not the subscriber is subscribed to that topic.
	topics map[int64]map[int64]bool
}

// NewBroker returns a new Broker
func NewBroker() *Broker {
	return &Broker{
		fromPublisherCh:   make(chan *pb.Publication, 32),
		toBrokerEchoChs:   NewBrokerEchoChannels(),
		fromBrokerEchoCh:  make(chan *pb.Publication, 32),
		toBrokerReadyChs:  NewBrokerReadyChannels(),
		fromBrokerReadyCh: make(chan *pb.Publication, 32),
		toSubscriberChs:   NewSubscriberPubChannels(),
		fromSubscriberCh:  make(chan *pb.SubRequest, 32),
		forwardSent:       make(map[int64]map[int64]bool),
		echoesSent:        make(map[int64]map[int64]bool),
		echoesReceived:    make(map[int64]map[int64]map[int64][]byte),
		readiesSent:       make(map[int64]map[int64]bool),
		readiesReceived:   make(map[int64]map[int64]map[int64][]byte),
		topics:            make(map[int64]map[int64]bool),
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
	pb.RegisterInterBrokerServer(grpcServer, broker)
	broker.ConnectToOtherBrokers(endpoint)
	go broker.handleMessages()

	fmt.Printf("Preparing to serve incoming requests.\n")
	err = grpcServer.Serve(listener)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func (b *Broker) ConnectToOtherBrokers(endpoint string) {

	brokerAddrs := []string{"localhost:11111", "localhost:11112", "localhost:11113", "localhost:11114"}

	for i, addr := range brokerAddrs {
		if addr != endpoint {
			go b.ConnectToBroker(int64(i), addr)
		}
	}

	for len(b.toBrokerEchoChs.chs) < 3 {
		fmt.Printf("Waiting for connections...\n")
		time.Sleep(time.Second)
	}
	fmt.Printf("...done\n")
}

func (b *Broker) ConnectToBroker(brokerID int64, brokerAddr string) {
	fmt.Printf("Trying to connect to %v\n", brokerAddr)
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure(), grpc.WithBlock())

	conn, err := grpc.Dial(brokerAddr, opts...)
	if err != nil {
		fmt.Printf("Error while connecting to server: %v\n", err)
		return
	}
	defer conn.Close()

	client := pb.NewInterBrokerClient(conn)
	toBrokerEchoCh := b.toBrokerEchoChs.AddBrokerEchoChannel(brokerID)
	toBrokerReadyCh := b.toBrokerReadyChs.AddBrokerReadyChannel(brokerID)

	for {
		select {
		case pub := <-toBrokerEchoCh:
			_, err := client.Echo(context.Background(), pub)
			if err != nil {
			}
		case pub := <-toBrokerReadyCh:
			_, err := client.Ready(context.Background(), pub)
			if err != nil {
			}
		}
	}
}

// Publish handles incoming Publish requests from publishers
func (b *Broker) Publish(ctx context.Context, pub *pb.Publication) (*pb.PubResponse, error) {

	b.fromPublisherCh <- pub

	return &pb.PubResponse{AlphaReached: false}, nil
}

// Echo handles incoming RBR echo requests from other brokers
func (b *Broker) Echo(ctx context.Context, pub *pb.Publication) (*pb.EchoResponse, error) {
	b.fromBrokerEchoCh <- pub
	return &pb.EchoResponse{}, nil
}

// Ready handles incoming RBR ready requests from other brokers
func (b *Broker) Ready(ctx context.Context, pub *pb.Publication) (*pb.ReadyResponse, error) {
	b.fromBrokerReadyCh <- pub
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

	id := req.SubscriberID
	ch := b.toSubscriberChs.AddSubscriberPubChannel(id)

	// Add initial subscribe for processing
	b.fromSubscriberCh <- req

	// Write loop
	go func() {
		for {
			select {
			case pub := <-ch:
				err := stream.Send(pub)
				if err != nil {
					b.toSubscriberChs.RemoveSubscriberPubChannel(id)
					break
				}
			}
		}
	}()

	// Read loop
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			b.toSubscriberChs.RemoveSubscriberPubChannel(id)
			return err
		} else if err != nil {
			b.toSubscriberChs.RemoveSubscriberPubChannel(id)
			return err
		}
		b.fromSubscriberCh <- req
	}

	return nil
}

// handleMessages handles incoming messages
func (b Broker) handleMessages() {
	for {
		select {
		// If it's a publish request
		case req := <-b.fromPublisherCh:
			if req.PubType == common.AB {
				// Handle an Authenticated Broadcast publish request
				b.handleAbPublish(req)
			} else if req.PubType == common.BRB {
				// Handle a Bracha's Reliable Broadcast publish request
				b.handleBrbPublish(req)
			} else if req.PubType == common.Chain {
				// Handle a Chain publish request
			}
		case req := <-b.fromBrokerEchoCh:
			b.handleEcho(req)
		case req := <-b.fromBrokerReadyCh:
			b.handleReady(req)
		case req := <-b.fromSubscriberCh:
			b.handleTopicChange(req)
		}
	}
}

func (b Broker) handleTopicChange(req *pb.SubRequest) {
	fmt.Printf("Changing topics for subscriber %v.\n", req.SubscriberID)

	if b.topics[req.SubscriberID] == nil {
		b.topics[req.SubscriberID] = make(map[int64]bool)
	}

	for i := range b.topics[req.SubscriberID] {
		b.topics[req.SubscriberID][i] = false
	}

	for _, topic := range req.Topics {
		b.topics[req.SubscriberID][topic] = true
	}
}
