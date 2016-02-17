package main

import (
	"fmt"
	"sync"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// Subscribers is a struct containing a map of subscribers' information.
type Subscribers struct {
	sync.RWMutex
	subscribers      map[int64]SubscriberInfo
}

// NewSubscribers returns a new Subscribers.
func NewSubscribers() *Subscribers {
	return &Subscribers{
		subscribers:      make(map[int64]SubscriberInfo),
	}
}

// SubscriberInfo is a struct containing information about the subscriber.
type SubscriberInfo struct {
	id             int64
	addr           string
	toSubscriberCh chan *pb.Publication
	topics         map[int64]bool
}

// AddSubscriberInfo adds a new SubscriberInfo to Subscribers. It returns the new SubscriberInfo.
// It takes as input the Subscriber ID, address, and a slice of topics.
func (s *Subscribers) AddSubscriberInfo(id int64, addr string, topics []int64) SubscriberInfo {
	fmt.Printf("Subscriber information for subscriber %v added.\n", id)

	subscriberInfo := SubscriberInfo{
		id:             id,
		addr:           addr,
		toSubscriberCh: make(chan *pb.Publication, 32),
		topics:         make(map[int64]bool),
	}
	for _, topic := range topics {
		subscriberInfo.topics[topic] = true
	}

	s.Lock()
	defer s.Unlock()

	s.subscribers[id] = subscriberInfo

	return subscriberInfo
}

// RemoveSubscriberInfo removes a SubscriberInfo from Subscribers.
// It takes as input the Subscriber ID
func (s *Subscribers) RemoveSubscriberInfo(id int64) {
	fmt.Printf("Subscriber information for subscriber %v removed.\n", id)

	s.Lock()
	s.Unlock()

	delete(s.subscribers, id)
}

// ChangeTopics changes the Subscriber's topics to the ones provided.
// It takes as input the Subscriber ID and a list of topics.
func (s *Subscribers) ChangeTopics(id int64, topics []int64) {
	fmt.Printf("Changing topics for subscriber %v.\n", id)

	s.Lock()
	s.Unlock()

	for i := range s.subscribers[id].topics {
		s.subscribers[id].topics[i] = false
	}

	for _, topic := range topics {
		s.subscribers[id].topics[topic] = true
	}
}
