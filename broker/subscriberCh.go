package main

import (
	"fmt"
	"sync"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// ToSubscriberChannels is a struct containing channels for sending publications to subscribers.
type ToSubscriberChannels struct {
	sync.RWMutex
	chs map[int64]chan *pb.Publication
}

// NewToSubscriberChannels returns a new ToSubscriberChannels.
func NewToSubscriberChannels() *ToSubscriberChannels {
	return &ToSubscriberChannels{
		chs: make(map[int64]chan *pb.Publication),
	}
}

// AddToSubscriberChannel adds a channel to ToSubscriberChannels.
// It also returns the channel. It takes as input the ID of the subscriber that will
// receive publications.
func (s *ToSubscriberChannels) AddToSubscriberChannel(id int64) chan *pb.Publication {
	fmt.Printf("To subscriber channel %v added.\n", id)
	ch := make(chan *pb.Publication, 32)

	s.Lock()
	defer s.Unlock()

	s.chs[id] = ch

	return ch
}

// RemoveToSubscriberChannel removes a channel from ToSubscriberChannels.
// It takes as input the ID of the subscribers that will no longer receive publications.
func (s *ToSubscriberChannels) RemoveToSubscriberChannel(id int64) {
	fmt.Printf("To subscriber channel %v removed.\n", id)

	s.Lock()
	s.Unlock()

	delete(s.chs, id)
}
