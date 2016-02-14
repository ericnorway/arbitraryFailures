package main

import (
	"fmt"
	"sync"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

type Subscribers struct {
	sync.RWMutex
	subs map[int64]Subscriber
}

func NewSubscribers() *Subscribers {
	return &Subscribers{
		subs: make(map[int64]Subscriber),
	}
}

type Subscriber struct {
	id      int64
	addr    string
	toSubCh chan *pb.Publication
	topics  map[int64]bool
}

func (s *Subscribers) addSubscriber(id int64, addr string, topics []int64) Subscriber {
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

func (s *Subscribers) removeSubscriber(id int64) {
	fmt.Printf("Subscriber connection information for client %v removed.\n", id)

	s.Lock()
	s.Unlock()

	delete(s.subs, id)
}

func (b *Subscribers) changeTopics(id int64, topics []int64) {

}
