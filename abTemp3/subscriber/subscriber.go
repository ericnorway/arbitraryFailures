package main

import (
	"fmt"
	"sync"

	pb "github.com/ericnorway/arbitraryFailures/abTemp3/proto"
)

type Subscriber struct {
	toBrokerChansMutex sync.RWMutex
	toBrokerChans map[string] chan *pb.SubRequest
	fromBrokerChan chan *pb.Publication
	
	// The first index references the publisher ID.
	// The second index references the publication ID.
	// The third index references the broker ID.
	// The byte slice contains the publication.
	pubsReceived map[int64] map[int64] map[int64] []byte
	
	// The first index references the publisher ID.
	// The second index references the publication ID.
	// The byte slice contains the publication.
	pubsLearned map[int64] map[int64] []byte
}

func NewSubscriber() *Subscriber {
	return &Subscriber{
		toBrokerChans: make(map[string] chan *pb.SubRequest),
		fromBrokerChan: make(chan *pb.Publication),
		pubsReceived: make(map[int64] map[int64] map[int64] []byte),
		pubsLearned: make(map[int64] map[int64] []byte),
	}
}

func main() {
	fmt.Printf("Subscriber started.\n")
	
	brokerAddrs := []string{"localhost:11111", "localhost:11112", "localhost:11113", "localhost:11114"}
	
	subscriber := NewSubscriber()
	
	subscriber.startBrokerClients(brokerAddrs)
	
	subscriber.processPublications()
	
	for {
	}
}
