package main

import (
	"fmt"
	"sync"
	"time"

	pb "github.com/ericnorway/arbitraryFailures/abTemp/proto"
)

type Publisher struct {
	abPubChansMutex sync.RWMutex
	abPubChans map[string] chan *pb.AbPubRequest
}

func NewPublisher() *Publisher {
	return &Publisher{
		abPubChans: make(map[string] chan *pb.AbPubRequest),
	}
}

func main() {
	fmt.Printf("Publisher started.\n")
	
	brokerAddrs := []string{"localhost:11111", "localhost:11112", "localhost:11113", "localhost:11114"}
	
	publisher := NewPublisher()
	
	publisher.AbStartBrokerClients(brokerAddrs)
	
	time.Sleep(time.Second)
	
	publisher.AbPublish(&pb.AbPubRequest{
		PublisherID: 1,
		PublicationID: 1,
		Publication: []byte(time.Now().String()),
	})
	time.Sleep(time.Second)
	
	publisher.AbPublish(&pb.AbPubRequest{
		PublisherID: 1,
		PublicationID: 2,
		Publication: []byte(time.Now().String()),
	})
	time.Sleep(time.Second)
	
	publisher.AbPublish(&pb.AbPubRequest{
		PublisherID: 1,
		PublicationID: 3,
		Publication: []byte(time.Now().String()),
	})
	time.Sleep(time.Second)
}
