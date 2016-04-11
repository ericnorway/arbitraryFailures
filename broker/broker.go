package broker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	// "google.golang.org/grpc/peer"
)

var toChannelLength = 256

// Broker is a struct containing channels used in communicating
// with read and write loops.
type Broker struct {
	localID   uint64
	localStr  string
	localAddr string

	alpha            uint64
	numberOfBrokers  uint64
	echoQuorumSize   uint64
	readyQuorumSize  uint64
	faultsTolerated  uint64
	chainRange       uint64
	maliciousPercent int
	random           *rand.Rand

	// For counting the number of publications (throughput)
	ToUserRecordCh chan bool

	// PUBLISHER VARIABLES
	publishersMutex sync.RWMutex
	publishers      map[uint64]publisherInfo
	fromPublisherCh chan pb.Publication

	// BROKER CHANNEL VARIABLES
	remoteBrokersMutex      sync.RWMutex
	remoteBrokers           map[uint64]brokerInfo
	remoteBrokerConnections uint64
	fromBrokerEchoCh        chan pb.Publication
	fromBrokerReadyCh       chan pb.Publication
	fromBrokerChainCh       chan pb.Publication

	// SUBSCRIBER CHANNEL VARIABLES
	subscribersMutex sync.RWMutex
	subscribers      map[uint64]subscriberInfo
	fromSubscriberCh chan pb.SubRequest

	// MESSAGE TRACKING VARIABLES

	// The first key references the publisher ID.
	// The second key references the publication ID.
	// The value contains whether or not it was sent yet.
	forwardSent map[uint64]map[int64]bool

	// The first key references the publisher ID.
	// The second key references the publication ID.
	// The value contains whether or not it was sent yet.
	echoesSent map[uint64]map[int64]bool

	// The first key references the publisher ID.
	// The second key references the publication ID.
	// The third key references the broker ID.
	// The value contains the content and topic of the publication.
	echoesReceived map[uint64]map[int64]map[uint64]string

	// The first key references the publisher ID.
	// The second key references the publication ID.
	// The value contains whether or not it was sent yet.
	readiesSent map[uint64]map[int64]bool

	// The first key references the publisher ID.
	// The second key references the publication ID.
	// The third key references the broker ID.
	// The value contains the content and topic of the publication.
	readiesReceived map[uint64]map[int64]map[uint64]string

	// The first key references the publisher ID.
	// The second key references the publication ID.
	// The value contains whether or not it was sent yet.
	chainSent map[uint64]map[int64]bool

	// The first key references the publisher ID.
	// The second key references the topic.
	// The value is a count of the messages received since the last history.
	alphaCounters map[uint64]map[uint64]uint64

	// The key is the first letter of the node type + node ID
	// For example a publisher node with ID of 3 would be "P3".
	chainNodes map[string]chainNode

	busyCh chan bool
	isBusy bool
}

// NewBroker returns a new Broker.
// It takes as input the local broker ID, the local address and port,
// the number of brokers, the alpha value, and the percent of malicious messages to send.
func NewBroker(localID uint64, localAddr string, numberOfBrokers uint64, alpha uint64, maliciousPercent int) *Broker {
	faultsTolerated := (numberOfBrokers - 1 ) / 3
	echoQuorumSize := (numberOfBrokers + faultsTolerated + 1) / 2
	readyQuorumSize := faultsTolerated + 1
	chainRange := faultsTolerated + 1

	return &Broker{
		localID:                 localID,
		localStr:                "B" + strconv.FormatUint(localID, 10),
		localAddr:               localAddr,
		alpha:                   alpha,
		numberOfBrokers:         numberOfBrokers,
		echoQuorumSize:          echoQuorumSize,
		readyQuorumSize:         readyQuorumSize,
		faultsTolerated:         faultsTolerated,
		chainRange:              chainRange,
		maliciousPercent:        maliciousPercent,
		random:                  rand.New(rand.NewSource(time.Now().Unix())),
		ToUserRecordCh:          make(chan bool, 32),
		publishers:              make(map[uint64]publisherInfo),
		fromPublisherCh:         make(chan pb.Publication, 32),
		remoteBrokers:           make(map[uint64]brokerInfo),
		remoteBrokerConnections: 0,
		fromBrokerEchoCh:        make(chan pb.Publication, 128),
		fromBrokerReadyCh:       make(chan pb.Publication, 128),
		fromBrokerChainCh:       make(chan pb.Publication, 32),
		subscribers:             make(map[uint64]subscriberInfo),
		fromSubscriberCh:        make(chan pb.SubRequest, 32),
		forwardSent:             make(map[uint64]map[int64]bool),
		echoesSent:              make(map[uint64]map[int64]bool),
		echoesReceived:          make(map[uint64]map[int64]map[uint64]string),
		readiesSent:             make(map[uint64]map[int64]bool),
		readiesReceived:         make(map[uint64]map[int64]map[uint64]string),
		chainSent:               make(map[uint64]map[int64]bool),
		alphaCounters:           make(map[uint64]map[uint64]uint64),
		chainNodes:              make(map[string]chainNode),
		busyCh:                  make(chan bool),
		isBusy:                  false,
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
	go b.checkBusy()

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
	toEchoCh, toReadyCh, toChainCh := b.addBrokerChannels(brokerID)

	// Write loop.
	for {
		select {
		case pub := <-toEchoCh:
			if b.maliciousPercent > 0 {
				altered := b.alterPublication(&pub)
				if altered {
					//fmt.Printf("Altered pub: %v\n", &pub)
				}
			}
			pub.MAC = common.CreatePublicationMAC(&pub, b.remoteBrokers[brokerID].key, common.Algorithm)

			_, err := client.Echo(context.Background(), &pub)
			if err != nil {
			}
		case pub := <-toReadyCh:
			if b.maliciousPercent > 0 {
				altered := b.alterPublication(&pub)
				if altered {
					//fmt.Printf("Altered pub: %v\n", &pub)
				}
			}
			pub.MAC = common.CreatePublicationMAC(&pub, b.remoteBrokers[brokerID].key, common.Algorithm)

			_, err := client.Ready(context.Background(), &pub)
			if err != nil {
			}
		case pub := <-toChainCh:
			if b.maliciousPercent > 0 {
				altered := b.alterPublication(&pub)
				if altered {
					//fmt.Printf("Altered pub: %v\n", &pub)
				}
			}
			pub.MAC = common.CreatePublicationMAC(&pub, b.remoteBrokers[brokerID].key, common.Algorithm)

			_, err := client.Chain(context.Background(), &pub)
			if err != nil {
			}
		}
	}
}

// Publish handles incoming Publish requests from publishers
func (b *Broker) Publish(ctx context.Context, pub *pb.Publication) (*pb.PubResponse, error) {
	if b.isBusy {
		return &pb.PubResponse{Status: pb.PubResponse_WAIT}, nil
	}

	publisher, exists := b.publishers[pub.PublisherID]

	// Check MAC
	if !exists || common.CheckPublicationMAC(pub, pub.MAC, publisher.key, common.Algorithm) == false {
		//fmt.Printf("***BAD MAC: Publish*** %v\n", *pub)
		return &pb.PubResponse{Status: pb.PubResponse_BAD_MAC}, nil
	}

	// If using alpha values (indicating a combination of algorithms)
	if b.alpha > 0 {
		if b.alphaCounters[pub.PublisherID] == nil {
			b.alphaCounters[pub.PublisherID] = make(map[uint64]uint64)
		}

		if pub.PubType == common.BRB {
			select {
			case b.fromPublisherCh <- *pub:
			}
			b.alphaCounters[pub.PublisherID][pub.TopicID] = 0
		} else {
			// Don't allow more than 2 * alpha publications for a topic and publisher without a history request
			if b.alphaCounters[pub.PublisherID][pub.TopicID] >= 2*b.alpha {
				return &pb.PubResponse{Status: pb.PubResponse_BLOCKED}, nil
			}

			select {
			case b.fromPublisherCh <- *pub:
			}

			b.alphaCounters[pub.PublisherID][pub.TopicID]++
			if b.alphaCounters[pub.PublisherID][pub.TopicID] == b.alpha {
				return &pb.PubResponse{Status: pb.PubResponse_HISTORY}, nil
			}
		}
	} else {
		select {
		case b.fromPublisherCh <- *pub:
		}
	}

	return &pb.PubResponse{Status: pb.PubResponse_OK}, nil
}

// Echo handles incoming BRB echo requests from other brokers
func (b *Broker) Echo(ctx context.Context, pub *pb.Publication) (*pb.EchoResponse, error) {
	remoteBroker, exists := b.remoteBrokers[pub.BrokerID]

	// Check MAC
	if !exists || common.CheckPublicationMAC(pub, pub.MAC, remoteBroker.key, common.Algorithm) == false {
		// fmt.Printf("***BAD MAC: Echo*** %v\n", *pub)
		return &pb.EchoResponse{}, nil
	}

	select {
	case b.fromBrokerEchoCh <- *pub:
	}

	return &pb.EchoResponse{}, nil
}

// Ready handles incoming BRB ready requests from other brokers
func (b *Broker) Ready(ctx context.Context, pub *pb.Publication) (*pb.ReadyResponse, error) {
	remoteBroker, exists := b.remoteBrokers[pub.BrokerID]

	// Check MAC
	if !exists || common.CheckPublicationMAC(pub, pub.MAC, remoteBroker.key, common.Algorithm) == false {
		// fmt.Printf("***BAD MAC: Ready*** %v\n", *pub)
		return &pb.ReadyResponse{}, nil
	}

	select {
	case b.fromBrokerReadyCh <- *pub:
	}

	return &pb.ReadyResponse{}, nil
}

// Chain handles incoming Chain requests from other brokers
func (b *Broker) Chain(ctx context.Context, pub *pb.Publication) (*pb.ChainResponse, error) {
	remoteBroker, exists := b.remoteBrokers[pub.BrokerID]

	// Check MAC
	if !exists || common.CheckPublicationMAC(pub, pub.MAC, remoteBroker.key, common.Algorithm) == false {
		// fmt.Printf("***BAD MAC: Chain*** %v\n", *pub)
		return &pb.ChainResponse{}, nil
	}

	select {
	case b.fromBrokerChainCh <- *pub:
	}

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

	subscriber, exists := b.subscribers[req.SubscriberID]

	// Check MAC
	if !exists || common.CheckSubscriptionMAC(req, req.MAC, subscriber.key, common.Algorithm) == false {
		//fmt.Printf("***BAD MAC: Subscribe*** %v\n", *req)
		return fmt.Errorf("***BAD MAC: Subscribe*** %v\n", *req)
	}

	id := req.SubscriberID
	ch := b.addToSubChannel(id)

	// Add initial subscribe for processing
	b.fromSubscriberCh <- *req

	// Write loop
	go func() {
		for {
			select {
			case pub := <-ch:
				if b.maliciousPercent > 0 {
					altered := b.alterPublication(&pub)
					if altered {
						//fmt.Printf("Altered pub: %v\n", &pub)
					}
				}

				pub.MAC = common.CreatePublicationMAC(&pub, b.subscribers[id].key, common.Algorithm)
				// fmt.Printf("Send Publication %v, Publisher %v, Broker %v to Subscriber %v.\n", pub.PublicationID, pub.PublisherID, pub.BrokerID, id)

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

		// Check MAC
		if common.CheckSubscriptionMAC(req, req.MAC, subscriber.key, common.Algorithm) == false {
			//fmt.Printf("***BAD MAC: Subscribe*** %v\n", *req)
			continue
		}

		select {
		case b.fromSubscriberCh <- *req:
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
				b.handleAbPublish(&req)
			} else if req.PubType == common.BRB {
				// Handle a Bracha's Reliable Broadcast publish request
				b.handleBrbPublish(&req)
			} else if req.PubType == common.Chain {
				// Handle a Chain publish request
				b.handleChainPublish(&req)
			}
		case req := <-b.fromBrokerEchoCh:
			b.handleEcho(&req)
		case req := <-b.fromBrokerReadyCh:
			b.handleReady(&req)
		case req := <-b.fromBrokerChainCh:
			b.handleChainPublish(&req)
		case req := <-b.fromSubscriberCh:
			b.handleSubscribe(&req)
		}
	}
}

// handleSubscribe handles a subscription request. It updates the topics.
// It takes as input the subscription request.
func (b Broker) handleSubscribe(req *pb.SubRequest) {
	b.changeTopics(req)
}

// alterPublication will maliciously alter a publications information.
// It returns true if the publication was altered.
// It takes as input the publication.
func (b *Broker) alterPublication(pub *pb.Publication) bool {
	r := b.random.Intn(101)

	if r <= b.maliciousPercent {
		var alterType int

		if len(pub.ChainMACs) > 0 {
			alterType = r % 6
		} else {
			alterType = r % 5
		}
		switch alterType {
		case 0:
			pub.PublicationID = pub.PublicationID + 1
		case 1:
			pub.PublisherID = pub.PublisherID + 1
		case 2:
			pub.TopicID = pub.TopicID + 1
		case 3:
			pub.BrokerID = pub.BrokerID + 1
		case 4:
			if len(pub.Contents) > 0 {
				pub.Contents[0] = []byte("Bad message")
			}
		case 5:
			pub.ChainMACs = nil
		}

		return true
	}

	return false
}

// incrementPublicationCount places a message on the ToUserRecordCh each time a publication
// is finished processing. This is used in calculating througput. It takes as input the publication,
// in case the publication is a history publication.
func (b *Broker) incrementPublicationCount(pub *pb.Publication) {
	select {
	case b.ToUserRecordCh <- true:
	default:
		// Use the default case just in case the user isn't reading from this channel
		// and the channel fills up.
	}

	if len(pub.Contents) > 1 {
		b.incrementPublicationCountByHistory(pub)
	}
}

// incrementPublicationCountByHistory places a message on the ToUserRecordCh for each missed publication
// in a history publication. This is used in calculating througput. It takes as input the publication.
func (b *Broker) incrementPublicationCountByHistory(pub *pb.Publication) {
	// Make the map so not trying to access nil reference
	if b.forwardSent[pub.PublisherID] == nil {
		b.forwardSent[pub.PublisherID] = make(map[int64]bool)
	}

	// Make the map so not trying to access nil reference
	if b.chainSent[pub.PublisherID] == nil {
		b.chainSent[pub.PublisherID] = make(map[int64]bool)
	}

	// For each publication in the history
	for _, histPub := range pub.Contents {

		if len(histPub) < 8 {
			continue
		}

		buf := bytes.NewBuffer(histPub)

		publicationID, _ := binary.ReadVarint(buf)

		// If a quorum has not been reached yet for this individual publication in the history.
		if b.forwardSent[pub.PublisherID][publicationID] == false && b.chainSent[pub.PublisherID][publicationID] == false {
			b.forwardSent[pub.PublisherID][publicationID] = true
			b.chainSent[pub.PublisherID][publicationID] = true

			//fmt.Printf("Learned publication %v from publisher %v from a history publication.\n", publicationID, pub.PublisherID)

			select {
			case b.ToUserRecordCh <- true:
			default:
				// Use the default case just in case the user isn't reading from this channel
				// and the channel fills up.
			}
		}
	}
}

// setBusy lets the broker know that one of the channels is filling up.
func (b *Broker) setBusy() {
	select {
	case b.busyCh <- true:
	default:
	}
}

// checkBusy checks the busy channel to see if any channels are busy.
// It then sets the busy flag for a certain amount of time, during which
// the broker accepts no new publications.
func (b *Broker) checkBusy() {

	ticker := time.NewTicker(20 * time.Millisecond)

	for {
		select {
		case <-b.busyCh:
			ticker = time.NewTicker(20 * time.Millisecond)
			b.isBusy = true
		case <-ticker.C:
			ticker.Stop()
			stillBusy := false

			// Check if the broker channels have gone down
			b.remoteBrokersMutex.RLock()
			for _, remoteBroker := range b.remoteBrokers {
				if len(remoteBroker.toEchoCh) > toChannelLength/2 ||
					len(remoteBroker.toReadyCh) > toChannelLength/2 ||
					len(remoteBroker.toChainCh) > toChannelLength/2 {
					stillBusy = true
				}
			}
			b.remoteBrokersMutex.RUnlock()

			// Check if the subscriber channels have gone down
			b.subscribersMutex.RLock()
			for _, subscriber := range b.subscribers {
				if len(subscriber.toCh) > toChannelLength/2 {
					stillBusy = true
				}
			}
			b.subscribersMutex.RUnlock()

			if stillBusy == false {
				b.isBusy = false
			} else {
				b.isBusy = true
				ticker = time.NewTicker(20 * time.Millisecond)
			}
		}
	}
}
