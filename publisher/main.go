package main

import (
	"fmt"
	"time"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// main starts a publisher and publishes three publications.
func main() {
	fmt.Printf("Publisher started.\n")

	parsedCorrectly := ParseArgs()
	if !parsedCorrectly {
		return
	}

	err := ReadConfigFile(*configFile)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}

	publisher := NewPublisher(localID)

	// Add broker information
	for i, key := range brokerKeys {
		id := int64(i)
		publisher.AddBroker(id, brokerAddresses[id], []byte(key))
	}

	publisher.StartBrokerClients()

	time.Sleep(time.Second)

	publisher.Publish(&pb.Publication{
		PubType:       common.AB,
		PublisherID:   localID,
		PublicationID: 1,
		Topic:         1,
		Content:       []byte(time.Now().String()),
	})
	time.Sleep(time.Second)

	publisher.Publish(&pb.Publication{
		PubType:       common.BRB,
		PublisherID:   localID,
		PublicationID: 2,
		Topic:         2,
		Content:       []byte(time.Now().String()),
	})
	time.Sleep(time.Second)

	publisher.Publish(&pb.Publication{
		PubType:       common.AB,
		PublisherID:   localID,
		PublicationID: 3,
		Topic:         1,
		Content:       []byte(time.Now().String()),
	})
	time.Sleep(time.Second)
}
