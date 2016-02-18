package main

import (
	"fmt"
	"sync"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

type SubscriberPubChannels struct {
	sync.RWMutex
	chs map[int64]chan *pb.Publication
}

func NewSubscriberPubChannels() *SubscriberPubChannels {
	return &SubscriberPubChannels{
		chs: make(map[int64]chan *pb.Publication),
	}
}

func (s *SubscriberPubChannels) AddSubscriberPubChannel(id int64) chan *pb.Publication {
	fmt.Printf("Subscriber pub channel %v added.\n", id)
	ch := make(chan *pb.Publication, 32)

	s.Lock()
	defer s.Unlock()

	s.chs[id] = ch

	return ch
}

func (s *SubscriberPubChannels) RemoveSubscriberPubChannel(id int64) {
	fmt.Printf("Subscriber pub channel %v removed.\n", id)

	s.Lock()
	s.Unlock()

	delete(s.chs, id)
}
