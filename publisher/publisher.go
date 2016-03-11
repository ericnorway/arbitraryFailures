package publisher

import (
	"fmt"
	"sync"
	"time"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Publisher is a struct containing a map of channels.
type Publisher struct {
	localID      uint64
	currentPubID int64

	brokersMutex      sync.RWMutex
	brokers           map[uint64]brokerInfo // The key is the BrokerID
	brokerConnections uint64

	historyRequestCh chan BrokerTopicPair
	addToHistoryCh   chan pb.Publication

	blockCh    chan BrokerTopicPair
	blockTopic map[uint64]bool // The key is TopicID

	ToUserRecordCh chan common.RecordTime

	// The key is the first letter of the node type + node ID
	// For example a publisher node with ID of 3 would be "P3"
	chainNodes map[string]chainNode
}

// BrokerTopicPair is a struct of BrokerID and TopicID that can be easily sent on a channel
type BrokerTopicPair struct {
	BrokerID uint64
	TopicID  uint64
}

// NewPublisher returns a new Publisher.
func NewPublisher(localID uint64) *Publisher {
	return &Publisher{
		localID:           localID,
		currentPubID:      0,
		brokers:           make(map[uint64]brokerInfo),
		brokerConnections: 0,
		historyRequestCh:  make(chan BrokerTopicPair, 8),
		addToHistoryCh:    make(chan pb.Publication, 8),
		blockCh:           make(chan BrokerTopicPair, 8),
		blockTopic:        make(map[uint64]bool),
		ToUserRecordCh:    make(chan common.RecordTime, 8),
		chainNodes:        make(map[string]chainNode),
	}
}

// Publish publishes a publication to all the brokers.
// It returns false if the topic ID is currently blocked, true otherwise.
// It takes as input a publication.
func (p *Publisher) Publish(pub *pb.Publication) bool {
	if p.blockTopic[pub.TopicID] == true {
		fmt.Printf("Blocked\n")
		return false
	}

	select {
	case p.addToHistoryCh <- *pub:
	}

	switch pub.PubType {
	case common.AB:
		p.handleAbPublish(pub)
	case common.BRB:
		p.handleBrbPublish(pub)
	case common.Chain:
		p.handleChainPublish(pub)
	}

	return true
}

// Start starts the publisher.
func (p *Publisher) Start() {
	go p.historyHandler()

	for _, broker := range p.brokers {
		go p.startBrokerClient(broker)
	}

	for p.brokerConnections < 3 {
		fmt.Printf("Waiting for connections...\n")
		time.Sleep(time.Second)
	}
	fmt.Printf("...done\n")
}

// startBrokerClient starts an individual broker client.
// It takes as input the broker information.
func (p *Publisher) startBrokerClient(broker brokerInfo) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure(), grpc.WithBlock())

	conn, err := grpc.Dial(broker.addr, opts...)
	if err != nil {
		fmt.Printf("Error while connecting to server: %v\n", err)
		return
	}
	defer conn.Close()

	client := pb.NewPubBrokerClient(conn)
	ch := p.addChannel(broker.id)

	for {
		select {
		case pub := <-ch:
			pub.MAC = common.CreatePublicationMAC(&pub, p.brokers[broker.id].key, common.Algorithm)

			// Handle publish request and response
			resp, err := client.Publish(context.Background(), &pub)
			if err != nil {
				fmt.Printf("Error publishing to %v, %v\n", broker.id, err)
				continue
			}

			if resp.RequestHistory == true {
				select {
				case p.historyRequestCh <- BrokerTopicPair{BrokerID: broker.id, TopicID: resp.TopicID}:
				}
			}

			if resp.Blocked == true {
				select {
				case p.blockCh <- BrokerTopicPair{BrokerID: broker.id, TopicID: resp.TopicID}:
				}
			}
		}
	}
}
