package publisher

import (
	"fmt"
	"strconv"
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
	localStr     string
	currentPubID int64

	brokersMutex      sync.RWMutex
	brokers           map[uint64]brokerInfo // The key is the BrokerID
	numberOfBrokers   uint64
	quorumSize        uint64
	faultsTolerated   uint64
	chainRange        uint64
	brokerConnections uint64

	// The key is the TopicID
	pubsSinceLastHistory map[uint64]uint64
	// The key is the TopicID. This is a map of slices.
	history map[uint64][]pb.Publication
	// The Publication ID for the history publications. Use negative numbers.
	historyID int64

	ToUserRecordCh chan common.RecordTime

	// The key is the first letter of the node type + node ID
	// For example a publisher node with ID of 3 would be "P3"
	chainNodes map[string]chainNode

	statusCh chan pb.PubResponse_Status
}

// HistoryRequestInfo
type HistoryRequestInfo struct {
	BrokerID uint64
	TopicID  uint64
	PubType  uint32
}

// NewPublisher returns a new Publisher.
func NewPublisher(localID uint64, numberOfBrokers uint64) *Publisher {
	faultsTolerated := (numberOfBrokers - 1) / 3
	quorumSize := 2*faultsTolerated + 1
	chainRange := faultsTolerated + 1

	return &Publisher{
		localID:              localID,
		localStr:             "P" + strconv.FormatUint(localID, 10),
		currentPubID:         0,
		brokers:              make(map[uint64]brokerInfo),
		numberOfBrokers:      numberOfBrokers,
		quorumSize:           quorumSize,
		faultsTolerated:      faultsTolerated,
		chainRange:           chainRange,
		brokerConnections:    0,
		pubsSinceLastHistory: make(map[uint64]uint64),
		history:              make(map[uint64][]pb.Publication),
		historyID:            int64(-1),
		ToUserRecordCh:       make(chan common.RecordTime, 16),
		chainNodes:           make(map[string]chainNode),
		statusCh:             make(chan pb.PubResponse_Status, numberOfBrokers),
	}
}

// Publish publishes a publication to all the brokers.
// It returns false if the topic ID is currently blocked or was not accepted by the brokers, true otherwise.
// It takes as input a publication.
func (p *Publisher) Publish(pub *pb.Publication) bool {

	accepted := p.publish(pub)

	switch accepted {
	case pb.PubResponse_OK:
		p.addToHistory(pub)
		return true
	case pb.PubResponse_HISTORY:
		p.addToHistory(pub)
		p.publishHistory(pub.TopicID)
		return true
	case pb.PubResponse_BLOCKED:
		p.publishHistory(pub.TopicID)
		return false
	case pb.PubResponse_BAD_MAC:
		return false
	case pb.PubResponse_WAIT:
		return false
	}

	return false
}

func (p *Publisher) publish(pub *pb.Publication) pb.PubResponse_Status {

	accepted := pb.PubResponse_OK

	switch pub.PubType {
	case common.AB:
		accepted = p.handleAbPublish(pub)
	case common.BRB:
		accepted = p.handleBrbPublish(pub)
	case common.Chain:
		accepted = p.handleChainPublish(pub)
	}

	return accepted
}

// Start starts the publisher.
func (p *Publisher) Start() {
	count := uint64(len(p.brokers))

	for _, broker := range p.brokers {
		go p.startBrokerClient(broker)
	}

	for p.brokerConnections < count {
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
				select {
				case p.statusCh <- -1:
				}
				continue
			}

			select {
			case p.statusCh <- resp.Status:
			}
		}
	}
}
