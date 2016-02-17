package main

import (
	"fmt"
	"sync"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// Publishers is a struct containing a map of publishers' information.
type Publishers struct {
	sync.RWMutex
	fromPublisherCh chan *pb.Publication
	publishers map[int64]PublisherInfo
}

// NewPublishers returns a new Publishers.
func NewPublishers() *Publishers {
	return &Publishers{
		fromPublisherCh: make(chan *pb.Publication, 32),
		publishers: make(map[int64]PublisherInfo),
	}
}

// PublisherInfo is a struct containing information about the publisher.
type PublisherInfo struct {
	id      int64
	addr    string
	toPublisherCh chan *pb.Publication
}

// AddPublisherInfo adds a new PublisherInfo to Publishers. It returns the new PublisherInfo.
// It takes as input the Publisher ID and address.
func (p *Publishers) AddPublisherInfo(id int64, addr string) PublisherInfo {
	fmt.Printf("Publisher information for publisher %v added.\n", id)

	publisherInfo := PublisherInfo{
		id:      id,
		addr:    addr,
		toPublisherCh: make(chan *pb.Publication, 32),
	}

	p.Lock()
	defer p.Unlock()

	p.publishers[id] = publisherInfo

	return publisherInfo
}

// RemovePublisherInfo removes a PublisherInfo from Publishers.
// It takes as input the Publisher ID
func (p *Publishers) RemovePublisherInfo(id int64) {
	fmt.Printf("Publisher information for publisher %v removed.\n", id)

	p.Lock()
	p.Unlock()

	delete(p.publishers, id)
}
