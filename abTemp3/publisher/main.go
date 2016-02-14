package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/ericnorway/arbitraryFailures/abTemp3/common"
	pb "github.com/ericnorway/arbitraryFailures/abTemp3/proto"
)

type Publisher struct {
	toBrokerChansMutex sync.RWMutex
	toBrokerChans map[string] chan *pb.Publication
}

func NewPublisher() *Publisher {
	return &Publisher{
		toBrokerChans: make(map[string] chan *pb.Publication),
	}
}

func main() {
	fmt.Printf("Publisher started.\n")
	
	brokerAddrs := []string{"localhost:11111", "localhost:11112", "localhost:11113", "localhost:11114"}
	
	publisher := NewPublisher()
	
	publisher.startBrokerClients(brokerAddrs)
	
	time.Sleep(time.Second)
	
	publisher.Publish(&pb.Publication{
		PubType: common.AB,
		PublisherID: 1,
		PublicationID: 1,
		Topic: 1,
		Content: []byte(time.Now().String()),
	})
	time.Sleep(time.Second)
	
	publisher.Publish(&pb.Publication{
		PubType: common.AB,
		PublisherID: 1,
		PublicationID: 2,
		Topic: 2,
		Content: []byte(time.Now().String()),
	})
	time.Sleep(time.Second)
	
	publisher.Publish(&pb.Publication{
		PubType: common.AB,
		PublisherID: 1,
		PublicationID: 3,
		Topic: 1,
		Content: []byte(time.Now().String()),
	})
	time.Sleep(time.Second)
}
