package broker

import (
	"fmt"
	"io"
	"net"
	"strings"
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
	localID   uint64
	localAddr string

	alpha           uint64
	numberOfServers uint64
	echoQuorumSize  uint64
	readyQuorumSize uint64
	faultsTolerated uint64

	// PUBLISHER VARIABLES
	publishersMutex sync.RWMutex
	publishers      map[uint64]publisherInfo
	fromPublisherCh chan *pb.Publication

	// BROKER CHANNEL VARIABLES
	remoteBrokersMutex      sync.RWMutex
	remoteBrokers           map[uint64]brokerInfo
	remoteBrokerConnections uint64
	fromBrokerEchoCh        chan *pb.Publication
	fromBrokerReadyCh       chan *pb.Publication
	fromBrokerChainCh       chan *pb.Publication

	// SUBSCRIBER CHANNEL VARIABLES
	subscribersMutex sync.RWMutex
	subscribers      map[uint64]subscriberInfo
	fromSubscriberCh chan *pb.SubRequest

	// MESSAGE TRACKING VARIABLES

	// The first index references the publisher ID.
	// The second index references the publication ID.
	// The bool contains whether or not it was sent yet.
	forwardSent map[uint64]map[int64]bool

	// The first index references the publisher ID.
	// The second index references the publication ID.
	// The bool contains whether or not it was sent yet.
	echoesSent map[uint64]map[int64]bool

	// The first index references the publisher ID.
	// The second index references the publication ID.
	// The third index references the broker ID.
	// The byte slice contains the content and topic of the publication.
	echoesReceived map[uint64]map[int64]map[uint64]string

	// The first index references the publisher ID.
	// The second index references the publication ID.
	// The bool contains whether or not it was sent yet.
	readiesSent map[uint64]map[int64]bool

	// The first index references the publisher ID.
	// The second index references the publication ID.
	// The third index references the broker ID.
	// The byte slice contains the content and topic of the publication.
	readiesReceived map[uint64]map[int64]map[uint64]string

	// The first index references the publisher ID.
	// The second index references the topic.
	// The value is a count of the messages received since the last history.
	alphaCounters map[uint64]map[uint64]uint64
}

// NewBroker returns a new Broker.
// It takes as input the local broker ID, the local address and port,
// and the alpha value.
func NewBroker(localID uint64, localAddr string, alpha uint64) *Broker {
	return &Broker{
		localID:                 localID,
		localAddr:               localAddr,
		alpha:                   alpha,
		numberOfServers:         4, // default
		echoQuorumSize:          3, // default
		readyQuorumSize:         2, // default
		faultsTolerated:         1, // default
		publishers:              make(map[uint64]publisherInfo),
		fromPublisherCh:         make(chan *pb.Publication, 32),
		remoteBrokers:           make(map[uint64]brokerInfo),
		remoteBrokerConnections: 0,
		fromBrokerEchoCh:        make(chan *pb.Publication, 32),
		fromBrokerReadyCh:       make(chan *pb.Publication, 32),
		fromBrokerChainCh:       make(chan *pb.Publication, 32),
		subscribers:             make(map[uint64]subscriberInfo),
		fromSubscriberCh:        make(chan *pb.SubRequest, 32),
		forwardSent:             make(map[uint64]map[int64]bool),
		echoesSent:              make(map[uint64]map[int64]bool),
		echoesReceived:          make(map[uint64]map[int64]map[uint64]string),
		readiesSent:             make(map[uint64]map[int64]bool),
		readiesReceived:         make(map[uint64]map[int64]map[uint64]string),
		alphaCounters:           make(map[uint64]map[uint64]uint64),
	}
}

// StartBroker starts a new Broker.
func (b *Broker) StartBroker() {
	fmt.Printf("Broker started.\n")

	pos := strings.Index(b.localAddr, ":")
	port := b.localAddr[pos:]

	listener, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("Listener started on %v\n", port)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterPubBrokerServer(grpcServer, b)
	pb.RegisterSubBrokerServer(grpcServer, b)
	pb.RegisterInterBrokerServer(grpcServer, b)
	b.connectToOtherBrokers()
	go b.handleMessages()

	fmt.Printf("*** Ready to serve incoming requests. ***\n")
	err = grpcServer.Serve(listener)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

// connectToOtherBrokers connects this broker to the other brokers.
func (b *Broker) connectToOtherBrokers() {

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
func (b *Broker) connectToBroker(brokerID uint64, brokerAddr string) {
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
			pub.MACs = make([][]byte, 1)
			pub.MACs[0] = common.CreatePublicationMAC(&pub, b.remoteBrokers[brokerID].key, common.Algorithm)

			_, err := client.Echo(context.Background(), &pub)
			if err != nil {
			}
		case pub := <-toBrokerReadyCh:
			pub.MACs = make([][]byte, 1)
			pub.MACs[0] = common.CreatePublicationMAC(&pub, b.remoteBrokers[brokerID].key, common.Algorithm)

			_, err := client.Ready(context.Background(), &pub)
			if err != nil {
			}
		}
	}
}

// Publish handles incoming Publish requests from publishers
func (b *Broker) Publish(ctx context.Context, pub *pb.Publication) (*pb.PubResponse, error) {
	publisher, exists := b.publishers[pub.PublisherID]

	// Check MAC
	if !exists || pub.MACs == nil || common.CheckPublicationMAC(pub, pub.MACs[0], publisher.key, common.Algorithm) == false {
		return &pb.PubResponse{Accepted: false, AlphaReached: false, TopicID: 0}, nil
	}

	select {
	case b.fromPublisherCh <- pub:
	}

	alphaReached := false

	// If using alpha values
	if b.alpha > 0 && pub.PubType != common.BRB {
		// Check if alpha has been reached for this publisher.
		//alphaReached = b.IncrementPubCount(pub.PublisherID)
	}

	return &pb.PubResponse{Accepted: true, AlphaReached: alphaReached, TopicID: pub.TopicID}, nil
}

// Echo handles incoming BRB echo requests from other brokers
func (b *Broker) Echo(ctx context.Context, pub *pb.Publication) (*pb.EchoResponse, error) {
	remoteBroker, exists := b.remoteBrokers[pub.BrokerID]

	// Check MAC
	if !exists || pub.MACs == nil || common.CheckPublicationMAC(pub, pub.MACs[0], remoteBroker.key, common.Algorithm) == false {
		return &pb.EchoResponse{}, nil
	}

	select {
	case b.fromBrokerEchoCh <- pub:
	}

	return &pb.EchoResponse{}, nil
}

// Ready handles incoming BRB ready requests from other brokers
func (b *Broker) Ready(ctx context.Context, pub *pb.Publication) (*pb.ReadyResponse, error) {
	remoteBroker, exists := b.remoteBrokers[pub.BrokerID]

	// Check MAC
	if !exists || pub.MACs == nil || common.CheckPublicationMAC(pub, pub.MACs[0], remoteBroker.key, common.Algorithm) == false {
		return &pb.ReadyResponse{}, nil
	}

	select {
	case b.fromBrokerReadyCh <- pub:
	}

	return &pb.ReadyResponse{}, nil
}

// Chain handles incoming Chain requests from other brokers
func (b *Broker) Chain(ctx context.Context, pub *pb.Publication) (*pb.ChainResponse, error) {
	remoteBroker, exists := b.remoteBrokers[pub.BrokerID]

	// Check MAC
	if !exists || pub.MACs == nil || common.CheckPublicationMAC(pub, pub.MACs[0], remoteBroker.key, common.Algorithm) == false {
		return &pb.ChainResponse{}, nil
	}

	//select {
	//case b.fromBrokerChainCh <- pub:
	//}

	return &pb.ChainResponse{}, nil
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
				pub.MACs = make([][]byte, 1)
				pub.MACs[0] = common.CreatePublicationMAC(&pub, b.subscribers[id].key, common.Algorithm)
				// fmt.Printf("Send AB Publish Publication %v, Publisher %v, Broker %v to Subscriber %v.\n", pub.PublicationID, pub.PublisherID, pub.BrokerID, id)

				err := stream.Send(&pub)
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

		select {
		case b.fromSubscriberCh <- req:
		}
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
	b.changeTopics(req)
}

func (b Broker) checkDoubleAlpha(publisherID uint64, topicID uint64) bool {
	return false
}

func (b Broker) incrementAlpha(publisherID uint64, topicID uint64) bool {
	return false
}
