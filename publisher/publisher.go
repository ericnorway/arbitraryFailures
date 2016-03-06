package publisher

import (
	"bytes"
	"encoding/binary"
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
}

type BrokerTopicPair struct {
	brokerID uint64
	topicID  uint64
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
	}
}

// Publish publishes a publication to all the brokers.
// It returns false if the topic ID is currently blocked, true otherwise.
// It takes as input a publication.
func (p *Publisher) Publish(pub *pb.Publication) bool {
	if p.blockTopic[pub.TopicID] == true {
		return false
	}

	select {
	case p.addToHistoryCh <- *pub:
	}

	p.brokersMutex.RLock()
	defer p.brokersMutex.RUnlock()

	for _, broker := range p.brokers {
		if broker.toCh != nil {
			select {
			case broker.toCh <- *pub:
			}
		}
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
			pub.MACs = make([][]byte, 1)
			pub.MACs[0] = common.CreatePublicationMAC(&pub, p.brokers[broker.id].key, common.Algorithm)

			// Handle publish request and response
			resp, err := client.Publish(context.Background(), &pub)
			if err != nil {
				fmt.Printf("Error publishing to %v, %v\n", broker.id, err)
				continue
			}

			if resp.RequestHistory == true {
				select {
				case p.historyRequestCh <- BrokerTopicPair{brokerID: broker.id, topicID: resp.TopicID}:
				}
			}

			if resp.Blocked == true {
				fmt.Printf("Block requested\n")
				select {
				case p.blockCh <- BrokerTopicPair{brokerID: broker.id, topicID: resp.TopicID}:
				}
			}
		}
	}
}

// historyHandler keeps a hisotry of the publications and sends alpha values when
// a quorum of alpha requests is reached.
func (p *Publisher) historyHandler() {
	// The key is TopicID
	pubsSinceLastHistory := make(map[uint64]uint64)
	// The first key is TopicID. The second key is BrokerID.
	historyRequests := make(map[uint64]map[uint64]bool)
	// The key in the TopicID
	history := make(map[uint64][]pb.Publication)
	// The Publication ID for the history publications. Use negative numbers.
	historyID := int64(-1)

	// The first key is TopicID. The second key is BrokerID
	blockRequests := make(map[uint64]map[uint64]bool)

	for {
		select {
		case pub := <-p.addToHistoryCh:
			history[pub.TopicID] = append(history[pub.TopicID], pub)
			pubsSinceLastHistory[pub.TopicID]++
		case pair := <-p.historyRequestCh:
			if historyRequests[pair.topicID] == nil {
				historyRequests[pair.topicID] = make(map[uint64]bool)
			}

			// If a history Publication was recently sent, ignore this request.
			if pubsSinceLastHistory[pair.topicID] < 2 {
				continue
			}

			historyRequests[pair.topicID][pair.brokerID] = true
			if len(historyRequests[pair.topicID]) > len(p.brokers)/2 {
				// Create the publication.
				pub := &pb.Publication{
					PubType:       common.BRB,
					PublisherID:   p.localID,
					PublicationID: historyID,
					TopicID:       pair.topicID,
					Contents: [][]byte{
						[]byte(" "),
					},
				}

				// For all the publications since the last history
				length := uint64(len(history[pub.TopicID]))
				for i := pubsSinceLastHistory[pub.TopicID]; i > 0; i-- {
					var buf bytes.Buffer
					publicationID := make([]byte, 8)

					// Write publication ID and contents to buffer
					binary.PutVarint(publicationID, history[pub.TopicID][length-i].PublicationID)
					buf.Write(publicationID)
					buf.Write(history[pub.TopicID][length-i].Contents[0])

					// Add to the contents
					pub.Contents = append(pub.Contents, buf.Bytes())
				}

				historyID--

				// Reset these
				historyRequests[pair.topicID] = make(map[uint64]bool)

				p.brokersMutex.RLock()
				for _, broker := range p.brokers {
					if broker.toCh != nil {
						select {
						case broker.toCh <- *pub:
						}
					}
				}
				p.brokersMutex.RUnlock()

				p.blockTopic[pair.topicID] = false
				pubsSinceLastHistory[pub.TopicID] = 0
			}
		case pair := <-p.blockCh:
			if blockRequests[pair.topicID] == nil {
				blockRequests[pair.topicID] = make(map[uint64]bool)
			}

			blockRequests[pair.topicID][pair.brokerID] = true
			// If more than one broker is blocking
			if len(blockRequests[pair.topicID]) > 1 {
				p.blockTopic[pair.topicID] = true
			}
		}
	}
}
