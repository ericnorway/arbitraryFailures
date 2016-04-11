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
	chainRange   int

	brokersMutex      sync.RWMutex
	brokers           map[uint64]brokerInfo // The key is the BrokerID
	numberOfBrokers   uint64
	quorumSize        uint64
	faultsTolerated   uint64
	brokerConnections uint64

	historyRequestCh chan HistoryRequestInfo
	addToHistoryCh   chan pb.Publication

	blockCh    chan HistoryRequestInfo
	blockTopic map[uint64]bool // The key is TopicID

	ToUserRecordCh chan common.RecordTime

	// The key is the first letter of the node type + node ID
	// For example a publisher node with ID of 3 would be "P3"
	chainNodes map[string]chainNode

	acceptedCh chan bool
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

	return &Publisher{
		localID:           localID,
		localStr:          "P" + strconv.FormatUint(localID, 10),
		currentPubID:      0,
		chainRange:        common.ChainRange,
		brokers:           make(map[uint64]brokerInfo),
		numberOfBrokers:   numberOfBrokers,
		quorumSize:        quorumSize,
		faultsTolerated:   faultsTolerated,
		brokerConnections: 0,
		historyRequestCh:  make(chan HistoryRequestInfo, 8),
		addToHistoryCh:    make(chan pb.Publication, 8),
		blockCh:           make(chan HistoryRequestInfo, 8),
		blockTopic:        make(map[uint64]bool),
		ToUserRecordCh:    make(chan common.RecordTime, 8),
		chainNodes:        make(map[string]chainNode),
		acceptedCh:        make(chan bool, 8),
	}
}

// Publish publishes a publication to all the brokers.
// It returns false if the topic ID is currently blocked or was not accepted by the brokers, true otherwise.
// It takes as input a publication.
func (p *Publisher) Publish(pub *pb.Publication) bool {
	if p.blockTopic[pub.TopicID] == true {
		fmt.Printf("Blocked\n")
		return false
	}

	select {
	case p.addToHistoryCh <- *pub:
	}

	accepted := false

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
				select {
				case p.acceptedCh <- false:
				}
				continue
			}

			accepted := false

			switch resp.Status {
			case pb.PubResponse_OK:
				accepted = true
			case pb.PubResponse_HISTORY:
				accepted = true
				select {
				case p.historyRequestCh <- HistoryRequestInfo{
					BrokerID: broker.id,
					TopicID:  pub.TopicID,
					PubType:  pub.PubType,
				}:
				}
			case pb.PubResponse_BLOCKED:
				accepted = false
				select {
				case p.blockCh <- HistoryRequestInfo{
					BrokerID: broker.id,
					TopicID:  pub.TopicID,
					PubType:  pub.PubType,
				}:
				}
			case pb.PubResponse_WAIT:
				accepted = false
			case pb.PubResponse_BAD_MAC:
				accepted = false
			default:
				accepted = false
			}

			select {
			case p.acceptedCh <- accepted:
			}
		}
	}
}
