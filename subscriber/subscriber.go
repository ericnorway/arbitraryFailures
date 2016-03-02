package subscriber

import (
	"fmt"
	"io"
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
	localID uint64

	brokersMutex      sync.RWMutex
	brokers           map[uint64]brokerInfo
	brokerConnections int64

	fromBrokerChan chan *pb.Publication
	ToUser         chan *pb.Publication
	FromUser       chan *pb.SubRequest

	// The first index references the publisher ID.
	// The second index references the publication ID.
	// The third index references the broker ID.
	// The byte slice contains the publication.
	pubsReceived map[uint64]map[int64]map[uint64][]byte

	// The first index references the publisher ID.
	// The second index references the publication ID.
	// The byte slice contains the publication.
	pubsLearned map[uint64]map[int64][]byte

	topics map[uint64]bool
}

// NewSubscriber returns a new Subscriber.
func NewSubscriber(localID uint64) *Subscriber {
	return &Subscriber{
		localID:           localID,
		brokers:           make(map[uint64]brokerInfo),
		brokerConnections: 0,
		fromBrokerChan:    make(chan *pb.Publication, 8),
		ToUser:            make(chan *pb.Publication, 8),
		FromUser:          make(chan *pb.SubRequest, 8),
		pubsReceived:      make(map[uint64]map[int64]map[uint64][]byte),
		pubsLearned:       make(map[uint64]map[int64][]byte),
		topics:            make(map[uint64]bool),
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
			select {
			case subReq := <-ch:
				// TODO: Add MAC

				err = stream.Send(&subReq)
				if err != nil {
					return
				}
			}
		}
	}()

	// Read loop
	go func() {
		for {
			pub, err := stream.Recv()
			// fmt.Printf("Received publication %v from publisher %v and broker %v.\n", pub.PublicationID, pub.PublisherID, pub.BrokerID)

			tempBroker, exists := s.brokers[pub.BrokerID]

			if !exists || pub.MACs == nil || common.CheckPublicationMAC(pub, pub.MACs[0], tempBroker.key, common.Algorithm) == false {
				continue
			}

			if err == io.EOF {
				s.removeChannel(broker.id)
				break
			}
			if err != nil {
				s.removeChannel(broker.id)
				break
			}

			select {
			case s.fromBrokerChan <- pub:
			}
		}
	}()

	select {
	// Send the initial subscribe request.
	case ch <- pb.SubRequest{
		SubscriberID: s.localID,
		Topics:       topics,
	}:
	}

	return true
}

// Start processes incoming publications from the brokers.
func (s *Subscriber) Start() {
	for _, broker := range s.brokers {
		go s.startBrokerClient(broker)
	}

	for s.brokerConnections < 3 {
		fmt.Printf("Waiting for connections...\n")
		time.Sleep(time.Second)
	}
	fmt.Printf("...done\n")

	s.processMessages()
}

// processMessages processes messages from brokers and from the user.
func (s *Subscriber) processMessages() {
	for {
		select {
		case pub := <-s.fromBrokerChan:
			if pub.PubType == common.AB {
				s.processAbPublication(pub)
			} else if pub.PubType == common.BRB {
				s.processBrbPublication(pub)
			}
		case sub := <-s.FromUser:
			s.brokersMutex.RLock()
			for _, broker := range s.brokers {
				select {
				case broker.toCh <- *sub:
				}
			}
			s.brokersMutex.RUnlock()
		}
	}
}

// processAbPublication processes an Authenticated Broadcast publication.
// It takes as input a publication.
func (s *Subscriber) processAbPublication(pub *pb.Publication) {
	// Make the map so not trying to access nil reference
	if s.pubsReceived[pub.PublisherID] == nil {
		s.pubsReceived[pub.PublisherID] = make(map[int64]map[uint64][]byte)
	}
	// Make the map so not trying to access nil reference
	if s.pubsReceived[pub.PublisherID][pub.PublicationID] == nil {
		s.pubsReceived[pub.PublisherID][pub.PublicationID] = make(map[uint64][]byte)
	}
	// Publication has not been received yet for this publisher ID, publication ID, broker ID
	if s.pubsReceived[pub.PublisherID][pub.PublicationID][pub.BrokerID] == nil {
		// So record it
		s.pubsReceived[pub.PublisherID][pub.PublicationID][pub.BrokerID] = pub.Content
		// Check if there is a quorum yet for this publisher ID and publication ID
		foundQuorum := s.checkQuorum(pub.PublisherID, pub.PublicationID, 3)

		if foundQuorum {
			select {
			case s.ToUser <- pub:
			}
		}
	}
}

// processBrbPublication processes a Bracha's Reliable Broadcast publication.
// It takes as input a publication.
func (s *Subscriber) processBrbPublication(pub *pb.Publication) {
	// Make the map so not trying to access nil reference
	if s.pubsReceived[pub.PublisherID] == nil {
		s.pubsReceived[pub.PublisherID] = make(map[int64]map[uint64][]byte)
	}
	// Make the map so not trying to access nil reference
	if s.pubsReceived[pub.PublisherID][pub.PublicationID] == nil {
		s.pubsReceived[pub.PublisherID][pub.PublicationID] = make(map[uint64][]byte)
	}
	// Publication has not been received yet for this publisher ID, publication ID, broker ID
	if s.pubsReceived[pub.PublisherID][pub.PublicationID][pub.BrokerID] == nil {
		// So record it
		s.pubsReceived[pub.PublisherID][pub.PublicationID][pub.BrokerID] = pub.Content
		// Check if there is a quorum yet for this publisher ID and publication ID
		foundQuorum := s.checkQuorum(pub.PublisherID, pub.PublicationID, 3)

		if foundQuorum {
			select {
			case s.ToUser <- pub:
			}
		}
	}
}

// checkQuorum checks that a quorum has been received for a specific publisher and publication.
// It return true if a quorum has been found, false otherwise. It takes as input
// the publisher ID, publication ID, and quorum size.
func (s *Subscriber) checkQuorum(publisherID uint64, publicationID int64, quorumSize uint) bool {
	// It's nil, so nothing to check.
	if s.pubsReceived[publisherID] == nil {
		s.pubsReceived[publisherID] = make(map[int64]map[uint64][]byte)
		return false
	}
	// It's nil, so nothing to check.
	if s.pubsReceived[publisherID][publicationID] == nil {
		s.pubsReceived[publisherID][publicationID] = make(map[uint64][]byte)
		return false
	}

	// Make the map so not trying to access nil reference
	if s.pubsLearned[publisherID] == nil {
		s.pubsLearned[publisherID] = make(map[int64][]byte)
	}
	// If already learned this publication
	if s.pubsLearned[publisherID][publicationID] != nil {
		// fmt.Printf("Already learned publication %v from publisher %v.\n", publicationID, publisherID)
		return false
	}

	// Just a temporary map to help with checking for a quorum. It keeps track of the number of each
	// publication value with this publisher ID and publication ID.
	countMap := make(map[string]uint)

	for _, publication := range s.pubsReceived[publisherID][publicationID] {
		pub := string(publication)
		countMap[pub] = countMap[pub] + 1
		if countMap[pub] >= quorumSize {
			s.pubsLearned[publisherID][publicationID] = publication
			// fmt.Printf("Learned publication %v from publisher %v.\n", publicationID, publisherID)
			return true
		}
	}

	return false
}
