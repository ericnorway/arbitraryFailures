package main

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
	id int64

	toBrokerChansMutex sync.RWMutex
	toBrokerChans      map[string]chan *pb.SubRequest
	fromBrokerChan     chan *pb.Publication
	ToUser             chan *pb.Publication
	FromUser           chan *pb.SubRequest

	// The first index references the publisher ID.
	// The second index references the publication ID.
	// The third index references the broker ID.
	// The byte slice contains the publication.
	pubsReceived map[int64]map[int64]map[int64][]byte

	// The first index references the publisher ID.
	// The second index references the publication ID.
	// The byte slice contains the publication.
	pubsLearned map[int64]map[int64][]byte

	topics map[int64]bool
}

// NewSubscriber returns a new Subscriber.
func NewSubscriber(id int64) *Subscriber {
	return &Subscriber{
		id:             id,
		toBrokerChans:  make(map[string]chan *pb.SubRequest),
		fromBrokerChan: make(chan *pb.Publication, 8),
		ToUser:         make(chan *pb.Publication, 8),
		FromUser:       make(chan *pb.SubRequest, 8),
		pubsReceived:   make(map[int64]map[int64]map[int64][]byte),
		pubsLearned:    make(map[int64]map[int64][]byte),
		topics:         make(map[int64]bool),
	}
}

// AddTopic adds a topic to the map. It takes as input the topic ID.
func (s *Subscriber) AddTopic(topic int64) {
	s.topics[topic] = true
}

// RemoveTopic removes a topic from the map. It takes as input the topic ID.
func (s *Subscriber) RemoveTopic(topic int64) {
	delete(s.topics, topic)
}

// StartBrokerClients starts the broker clients. It takes as input
// a slice of broker addresses.
func (s *Subscriber) StartBrokerClients(brokerAddrs []string) {
	for i := range brokerAddrs {
		go s.startBrokerClient(brokerAddrs[i])
	}

	for len(s.toBrokerChans) < 3 {
		fmt.Printf("Waiting for connections...\n")
		time.Sleep(time.Second)
	}
	fmt.Printf("...done\n")
}

// startBrokerClient starts an individual broker clients. It takes as input
// a broker address.
func (s *Subscriber) startBrokerClient(brokerAddr string) bool {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())

	conn, err := grpc.Dial(brokerAddr, opts...)
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

	var topics []int64
	for i := range s.topics {
		topics = append(topics, i)
	}

	// Send the initial subscribe request.
	err = stream.Send(&pb.SubRequest{
		SubscriberID: s.id,
		Topics:       topics,
	})
	if err != nil {
		return false
	}
	s.addChannel(brokerAddr)

	// Read loop
	go func() {
		for {
			pub, err := stream.Recv()
			if err == io.EOF {
				s.removeChannel(brokerAddr)
				break
			}
			if err != nil {
				s.removeChannel(brokerAddr)
				break
			}

			s.fromBrokerChan <- pub
		}
	}()

	// Write loop
	go func() {
		for {
			select {
			case subReq := <-s.FromUser:
				err = stream.Send(subReq)
				if err != nil {
					return
				}
			}
		}
	}()

	return true
}

// addChannel adds a channel to the map of to broker channels.
// It returns the new channel. It takes as input the address
// of the broker.
func (s *Subscriber) addChannel(addr string) chan *pb.SubRequest {
	fmt.Printf("Broker channel to %v added.\n", addr)

	s.toBrokerChansMutex.Lock()
	defer s.toBrokerChansMutex.Unlock()

	ch := make(chan *pb.SubRequest, 32)
	s.toBrokerChans[addr] = ch
	return ch
}

// removeChannel removes a channel from the map of to broker channels.
// It takes as input the address of the broker.
func (s *Subscriber) removeChannel(addr string) {
	fmt.Printf("Broker channel to %v removed.\n", addr)

	s.toBrokerChansMutex.Lock()
	s.toBrokerChansMutex.Unlock()

	delete(s.toBrokerChans, addr)
}

// ProcessPublications processes incoming publications from the brokers.
func (s *Subscriber) ProcessPublications() {

	for {
		select {
		case pub := <-s.fromBrokerChan:
			if pub.PubType == common.AB {
				s.processAbPublication(pub)
			} else if pub.PubType == common.BRB {
				s.processBrbPublication(pub)
			}
		default:
		}
	}
}

// processAbPublication processes an Authenticated Broadcast publication.
// It takes as input a publication.
func (s *Subscriber) processAbPublication(pub *pb.Publication) {
	// Make the map so not trying to access nil reference
	if s.pubsReceived[pub.PublisherID] == nil {
		s.pubsReceived[pub.PublisherID] = make(map[int64]map[int64][]byte)
	}
	// Make the map so not trying to access nil reference
	if s.pubsReceived[pub.PublisherID][pub.PublicationID] == nil {
		s.pubsReceived[pub.PublisherID][pub.PublicationID] = make(map[int64][]byte)
	}
	// Publication has not been received yet for this publisher ID, publication ID, broker ID
	if s.pubsReceived[pub.PublisherID][pub.PublicationID][pub.BrokerID] == nil {
		// So record it
		s.pubsReceived[pub.PublisherID][pub.PublicationID][pub.BrokerID] = pub.Content
		// Check if there is a quorum yet for this publisher ID and publication ID
		foundQuorum := s.checkQuorum(pub.PublisherID, pub.PublicationID, 3)

		if foundQuorum {
			s.ToUser <- pub
		}
	}
}

// processBrbPublication processes a Bracha's Reliable Broadcast publication.
// It takes as input a publication.
func (s *Subscriber) processBrbPublication(pub *pb.Publication) {
	// Make the map so not trying to access nil reference
	if s.pubsReceived[pub.PublisherID] == nil {
		s.pubsReceived[pub.PublisherID] = make(map[int64]map[int64][]byte)
	}
	// Make the map so not trying to access nil reference
	if s.pubsReceived[pub.PublisherID][pub.PublicationID] == nil {
		s.pubsReceived[pub.PublisherID][pub.PublicationID] = make(map[int64][]byte)
	}
	// Publication has not been received yet for this publisher ID, publication ID, broker ID
	if s.pubsReceived[pub.PublisherID][pub.PublicationID][pub.BrokerID] == nil {
		// So record it
		s.pubsReceived[pub.PublisherID][pub.PublicationID][pub.BrokerID] = pub.Content
		// Check if there is a quorum yet for this publisher ID and publication ID
		foundQuorum := s.checkQuorum(pub.PublisherID, pub.PublicationID, 3)

		if foundQuorum {
			s.ToUser <- pub
		}
	}
}

// checkQuorum checks that a quorum has been received for a specific publisher and publication.
// It return true if a quorum has been found, false otherwise. It takes as input
// the publisher ID, publication ID, and quorum size.
func (s *Subscriber) checkQuorum(publisherID int64, publicationID int64, quorumSize int) bool {
	// It's nil, so nothing to check.
	if s.pubsReceived[publisherID] == nil {
		s.pubsReceived[publisherID] = make(map[int64]map[int64][]byte)
		return false
	}
	// It's nil, so nothing to check.
	if s.pubsReceived[publisherID][publicationID] == nil {
		s.pubsReceived[publisherID][publicationID] = make(map[int64][]byte)
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
	countMap := make(map[string]int)

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
