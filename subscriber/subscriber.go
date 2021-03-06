package subscriber

import (
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Subscriber is a struct containing channels, a map of
// publications received, a map of publications learned,
// and a map of topics.
type Subscriber struct {
	localID  uint64
	localStr string

	brokersMutex      sync.RWMutex
	brokers           map[uint64]brokerInfo
	numberOfBrokers   uint64
	quorumSize        uint64
	faultsTolerated   uint64
	chainRange        uint64
	brokerConnections uint64

	fromBrokerCh  chan pb.Publication
	ToUserPubCh   chan pb.Publication
	FromUserSubCh chan pb.SubRequest

	// The first index references the publisher ID.
	// The second index references the publication ID.
	// The third index references the broker ID.
	// The string contains the publication.
	pubsReceived map[uint64]map[int64]map[uint64]string

	// The first index references the publisher ID.
	// The second index references the publication ID.
	// The string contains the publication.
	pubsLearned map[uint64]map[int64]string

	// The key is the Topic ID.
	// The value is whether or not the subscriber is interested in that topic.
	topics map[uint64]bool

	// The key is the first letter of the node type + node ID
	// For example a publisher node with ID of 3 would be "P3"
	chainNodes map[string]chainNode
}

// NewSubscriber returns a new Subscriber.
func NewSubscriber(localID uint64, numberOfBrokers uint64) *Subscriber {
	faultsTolerated := (numberOfBrokers - 1) / 3
	quorumSize := 2*faultsTolerated + 1
	chainRange := faultsTolerated + 1

	return &Subscriber{
		localID:           localID,
		localStr:          "S" + strconv.FormatUint(localID, 10),
		brokers:           make(map[uint64]brokerInfo),
		numberOfBrokers:   numberOfBrokers,
		quorumSize:        quorumSize,
		faultsTolerated:   faultsTolerated,
		chainRange:        chainRange,
		brokerConnections: 0,
		fromBrokerCh:      make(chan pb.Publication, 32),
		ToUserPubCh:       make(chan pb.Publication, 32),
		FromUserSubCh:     make(chan pb.SubRequest, 8),
		pubsReceived:      make(map[uint64]map[int64]map[uint64]string),
		pubsLearned:       make(map[uint64]map[int64]string),
		topics:            make(map[uint64]bool),
		chainNodes:        make(map[string]chainNode),
	}
}

// AddTopic adds a topic to the map. It takes as input the topic ID.
func (s *Subscriber) AddTopic(topic uint64) {
	s.topics[topic] = true
}

// RemoveTopic removes a topic from the map. It takes as input the topic ID.
func (s *Subscriber) RemoveTopic(topic uint64) {
	delete(s.topics, topic)
}

// startBrokerClient starts an individual broker clients. It takes as input
// broker information.
func (s *Subscriber) startBrokerClient(broker brokerInfo) bool {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())

	conn, err := grpc.Dial(broker.addr, opts...)
	if err != nil {
		fmt.Printf("Error while connecting to server: %v\n", err)
		return false
	}

	client := pb.NewSubBrokerClient(conn)

	stream, err := client.Subscribe(context.Background())
	if err != nil {
		fmt.Printf("Error while starting the Subscribe stream: %v\n", err)
		return false
	}

	var topics []uint64
	for i := range s.topics {
		topics = append(topics, i)
	}

	ch := s.addChannel(broker.id)

	// Write loop
	go func() {
		for {
			subReq := <-ch
			mac := common.CreateSubscriptionMAC(&subReq, broker.key)
			subReq.MAC = mac

			err = stream.Send(&subReq)
			if err != nil {
				return
			}
		}
	}()

	// Read loop
	go func() {
		for {
			pub, err := stream.Recv()
			// fmt.Printf("Received publication %v from publisher %v and broker %v.\n", pub.PublicationID, pub.PublisherID, pub.BrokerID)

			if err == io.EOF {
				s.removeChannel(broker.id)
				break
			}
			if err != nil {
				s.removeChannel(broker.id)
				break
			}

			tempBroker, exists := s.brokers[pub.BrokerID]

			if !exists || common.CheckPublicationMAC(pub, pub.MAC, tempBroker.key) == false {
				//fmt.Printf("***BAD MAC: Chain*** %v\n", *pub)
				continue
			}

			s.fromBrokerCh <- *pub
		}
	}()

	// Send the initial subscribe request.
	subReq := pb.SubRequest{
		SubscriberID: s.localID,
		TopicIDs:     topics,
	}
	mac := common.CreateSubscriptionMAC(&subReq, broker.key)
	subReq.MAC = mac
	ch <- subReq

	return true
}

// Start processes incoming publications from the brokers.
func (s *Subscriber) Start() {
	for _, broker := range s.brokers {
		go s.startBrokerClient(broker)
	}

	for s.brokerConnections < s.numberOfBrokers {
		fmt.Printf("Waiting for connections...\n")
		time.Sleep(time.Second)
	}
	fmt.Printf("...done\n")

	s.handlePublications()
}

// handlePublications processes messages from brokers and from the user.
func (s *Subscriber) handlePublications() {
	for {
		select {
		case pub := <-s.fromBrokerCh:
			if pub.PubType == common.AB {
				foundQuorum := s.handleAbPublication(&pub)
				if foundQuorum {
					s.ToUserPubCh <- pub
				}
			} else if pub.PubType == common.BRB {
				foundQuorum := s.handleBrbPublication(&pub)
				if foundQuorum {
					s.ToUserPubCh <- pub
				}
			} else if pub.PubType == common.Chain {
				macsVerified := s.handleChainPublication(&pub)
				if macsVerified {
					s.ToUserPubCh <- pub
				}
			}
		case sub := <-s.FromUserSubCh:
			s.brokersMutex.RLock()
			for _, broker := range s.brokers {
				broker.toCh <- sub
			}
			s.brokersMutex.RUnlock()
		}
	}
}

// checkQuorum checks that a quorum has been received for a specific publisher and publication.
// It return true if a quorum has been found, false otherwise. It takes as input
// the publisher ID and publication ID.
func (s *Subscriber) checkQuorum(publisherID uint64, publicationID int64) bool {
	// It's nil, so nothing to check.
	if s.pubsReceived[publisherID] == nil {
		s.pubsReceived[publisherID] = make(map[int64]map[uint64]string)
		return false
	}
	// It's nil, so nothing to check.
	if s.pubsReceived[publisherID][publicationID] == nil {
		s.pubsReceived[publisherID][publicationID] = make(map[uint64]string)
		return false
	}

	// Make the map so not trying to access nil reference
	if s.pubsLearned[publisherID] == nil {
		s.pubsLearned[publisherID] = make(map[int64]string)
	}
	// If already learned this publication
	if s.pubsLearned[publisherID][publicationID] != "" {
		// fmt.Printf("Already learned publication %v from publisher %v.\n", publicationID, publisherID)
		return false
	}

	// Just a temporary map to help with checking for a quorum. It keeps track of the number of each
	// publication value with this publisher ID and publication ID.
	countMap := make(map[string]uint64)
	for _, contents := range s.pubsReceived[publisherID][publicationID] {
		countMap[contents] = countMap[contents] + 1
		if countMap[contents] >= s.quorumSize {
			s.pubsLearned[publisherID][publicationID] = contents
			// fmt.Printf("Learned publication %v from publisher %v.\n", publicationID, publisherID)
			return true
		}
	}

	return false
}
