package main

import (
	"fmt"
	"sync"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

type ToSubscriberChannels struct {
	sync.RWMutex
	chs map[int64]chan *pb.Publication
}

func NewToSubscriberChannels() *ToSubscriberChannels {
	return &ToSubscriberChannels{
		chs: make(map[int64]chan *pb.Publication),
	}
}

func (s *ToSubscriberChannels) AddToSubscriberChannel(id int64) chan *pb.Publication {
	fmt.Printf("To subscriber channel %v added.\n", id)
	ch := make(chan *pb.Publication, 32)

	s.Lock()
	defer s.Unlock()

	s.chs[id] = ch

	return ch
}

func (s *ToSubscriberChannels) RemoveToSubscriberChannel(id int64) {
	fmt.Printf("To subscriber channel %v removed.\n", id)

	s.Lock()
	s.Unlock()

	delete(s.chs, id)
}
