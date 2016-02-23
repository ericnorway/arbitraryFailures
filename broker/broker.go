package main

import (
	"fmt"
	"io"
	"net"
	"sync"
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
	addr string

	numberOfServers int64
	echoQuorumSize  int64
	readyQuorumSize int64
	faultsTolerated int64

	// PUBLISHER VARIABLES
	publishersMutex sync.RWMutex
	publishers      map[int64]publisherInfo
	fromPublisherCh chan *pb.Publication

	// BROKER CHANNEL VARIABLES
	remoteBrokersMutex      sync.RWMutex
	remoteBrokers           map[int64]brokerInfo
	remoteBrokerConnections int64
	fromBrokerEchoCh        chan *pb.Publication
	fromBrokerReadyCh       chan *pb.Publication

	// SUBSCRIBER CHANNEL VARIABLES
	subscribersMutex sync.RWMutex
	subscribers      map[int64]subscriberInfo
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
	// The byte slice contains the content and topic of the publication.
	echoesReceived map[int64]map[int64]map[int64]string

	// The first index references the publisher ID.
	// The second index references the publication ID.
	// The bool contains whether or not it was sent yet.
	readiesSent map[int64]map[int64]bool

	// The first index references the publisher ID.
	// The second index references the publication ID.
	// The third index references the broker ID.
	// The byte slice contains the content and topic of the publication.
	readiesReceived map[int64]map[int64]map[int64]string

	// The first index references the subscriber ID.
	// The second index references the topic ID.
	// The bool contains whether or not the subscriber is subscribed to that topic.
	topics map[int64]map[int64]bool

	publisherKeys [][]byte
}

// NewBroker returns a new Broker
func NewBroker(addr string) *Broker {
	return &Broker{
		addr:                    addr,
		numberOfServers:         4, // default
		echoQuorumSize:          3, // default
		readyQuorumSize:         2, // default
		faultsTolerated:         1, // default
		publishers:              make(map[int64]publisherInfo),
		fromPublisherCh:         make(chan *pb.Publication, 32),
		remoteBrokers:           make(map[int64]brokerInfo),
		remoteBrokerConnections: 0,
		fromBrokerEchoCh:        make(chan *pb.Publication, 32),
		fromBrokerReadyCh:       make(chan *pb.Publication, 32),
		subscribers:             make(map[int64]subscriberInfo),
		fromSubscriberCh:        make(chan *pb.SubRequest, 32),
		forwardSent:             make(map[int64]map[int64]bool),
		echoesSent:              make(map[int64]map[int64]bool),
		echoesReceived:          make(map[int64]map[int64]map[int64]string),
		readiesSent:             make(map[int64]map[int64]bool),
		readiesReceived:         make(map[int64]map[int64]map[int64]string),
		topics:                  make(map[int64]map[int64]bool),
	}
}

// StartBroker starts a new Broker.
func (b *Broker) StartBroker() {
	fmt.Printf("Broker started.\n")

	listener, err := net.Listen("tcp", b.addr)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("Listener started on %v\n", b.addr)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterPubBrokerServer(grpcServer, b)
	pb.RegisterSubBrokerServer(grpcServer, b)
	pb.RegisterInterBrokerServer(grpcServer, b)
	b.connectToOtherBrokers(b.addr)
	go b.handleMessages()

	fmt.Printf("*** Ready to serve incoming requests. ***\n")
	err = grpcServer.Serve(listener)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

// connectToOtherBrokers connects this broker to the other brokers.
// It takes as input the local broker's address.
func (b *Broker) connectToOtherBrokers(localAddr string) {

	// Connect to all broker addresses except itself.
	for _, broker := range b.remoteBrokers {
		go b.connectToBroker(broker.id, broker.addr)
	}

	// Wait for connections to be established.
	for b.remoteBrokerConnections < 3 {
		fmt.Printf("Waiting for connections...\n")
		time.Sleep(time.Second)
	}
	fmt.Printf("...done\n")
}

// connectToBroker connects to a single broker.
// It takes as input the remote broker's ID and address.
func (b *Broker) connectToBroker(brokerID int64, brokerAddr string) {
	fmt.Printf("Trying to connect to %v\n", brokerAddr)
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure(), grpc.WithBlock())

	// Create a gRPC connection
	conn, err := grpc.Dial(brokerAddr, opts...)
	if err != nil {
		fmt.Printf("Error while connecting to server: %v\n", err)
		return
	}
	defer conn.Close()

	client := pb.NewInterBrokerClient(conn)
	toBrokerEchoCh, toBrokerReadyCh := b.addBrokerChannels(brokerID)

	// Write loop.
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
	//if b.publishers[pub.PublisherID] == PublisherInfo{} {
	//	return &pb.PubResponse{AlphaReached: false}, nil
	//}

	if pub.MACs == nil || common.CheckPublicationMAC(pub, pub.MACs[0], []byte("12345"), common.Algorithm) == false {
		return &pb.PubResponse{AlphaReached: false}, nil
	}
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
	ch := b.addToSubChannel(id)

	// Add initial subscribe for processing
	b.fromSubscriberCh <- req

	// Write loop
	go func() {
		for {
			select {
			case pub := <-ch:
				err := stream.Send(pub)
				if err != nil {
					b.removeToSubChannel(id)
					break
				}
			}
		}
	}()

	// Read loop
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			b.removeToSubChannel(id)
			return err
		} else if err != nil {
			b.removeToSubChannel(id)
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
			b.handleSubscribe(req)
		}
	}
}

// handleSubscribe handles a subscription request. It updates the topics.
// It takes as input the subscription request.
func (b Broker) handleSubscribe(req *pb.SubRequest) {
	// fmt.Printf("Changing topics for subscriber %v.\n", req.SubscriberID)

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
