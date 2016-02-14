package main

import (
	"fmt"
	"io"
	"sync"
	"time"

	pb "github.com/ericnorway/arbitraryFailures/proto"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type Subscriber struct {
	toBrokerChansMutex sync.RWMutex
	toBrokerChans      map[string]chan *pb.SubRequest
	fromBrokerChan     chan *pb.Publication

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

func NewSubscriber() *Subscriber {
	return &Subscriber{
		toBrokerChans:  make(map[string]chan *pb.SubRequest),
		fromBrokerChan: make(chan *pb.Publication),
		pubsReceived:   make(map[int64]map[int64]map[int64][]byte),
		pubsLearned:    make(map[int64]map[int64][]byte),
		topics:         make(map[int64]bool),
	}
}

func (s *Subscriber) AddTopic(topic int64) {
	s.topics[topic] = true
}

func (s *Subscriber) RemoveTopic(topic int64) {
	delete(s.topics, topic)
}

func (s *Subscriber) startBrokerClients(brokerAddrs []string) {
	for i := range brokerAddrs {
		go s.startBrokerClient(brokerAddrs[i])
	}

	for len(s.toBrokerChans) < 3 {
		fmt.Printf("Waiting for connections...\n")
		time.Sleep(time.Second)
	}
	fmt.Printf("...done\n")
}

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

	err = stream.Send(&pb.SubRequest{
		SubscriberID: 1,
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

	return true
}

func (s *Subscriber) addChannel(addr string) chan *pb.SubRequest {
	fmt.Printf("Broker channel to %v added.\n", addr)

	s.toBrokerChansMutex.Lock()
	defer s.toBrokerChansMutex.Unlock()

	ch := make(chan *pb.SubRequest, 32)
	s.toBrokerChans[addr] = ch
	return ch
}

func (s *Subscriber) removeChannel(addr string) {
	fmt.Printf("Broker channel to %v removed.\n", addr)

	s.toBrokerChansMutex.Lock()
	s.toBrokerChansMutex.Unlock()

	delete(s.toBrokerChans, addr)
}

func (s *Subscriber) processPublications() {

	for {
		select {
		case pub := <-s.fromBrokerChan:
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
				s.checkQuorum(pub.PublisherID, pub.PublicationID, 3)
			}
		default:
		}
	}
}

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
		fmt.Printf("Already learned publication %v from publisher %v.\n", publicationID, publisherID)
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
			fmt.Printf("Learned publication %v from publisher %v.\n", publicationID, publisherID)
			return true
		}
	}

	return false
}
