package main

import (
	"fmt"
	"sync"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// Subscribers is a struct containing a map of subscribers.
type Subscribers struct {
	sync.RWMutex
	subs map[int64]Subscriber
}

// NewSubscribers returns a new Subscribers.
func NewSubscribers() *Subscribers {
	return &Subscribers{
		subs: make(map[int64]Subscriber),
	}
}

// Subscriber is a struct containing information about the subscriber.
type Subscriber struct {
	id      int64
	addr    string
	toSubCh chan *pb.Publication
	topics  map[int64]bool
}

// AddSubscriber adds a new Subscriber to Subscribers. It returns the new Subscriber.
// It takes as input the Subsciber ID, address, and a slice of topics.
func (s *Subscribers) AddSubscriber(id int64, addr string, topics []int64) Subscriber {
	fmt.Printf("Subscriber connection information for client %v added.\n", id)

	sub := Subscriber{
		id:      id,
		addr:    addr,
		toSubCh: make(chan *pb.Publication, 32),
		topics:  make(map[int64]bool),
	}
	for _, topic := range topics {
		sub.topics[topic] = true
	}

	s.Lock()
	defer s.Unlock()

	s.subs[id] = sub

	return sub
}

// RemoveSubscriber removes a Subscriber from Subscribers.
// It takes as input the Subscriber ID
func (s *Subscribers) RemoveSubscriber(id int64) {
	fmt.Printf("Subscriber connection information for client %v removed.\n", id)

	s.Lock()
	s.Unlock()

	delete(s.subs, id)
}

// ChangeTopics changes the Subscriber's topics to the ones provided.
// It takes as input the Subscriber ID and a list of topics.
func (s *Subscribers) ChangeTopics(id int64, topics []int64) {

}
