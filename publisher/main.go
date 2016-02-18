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

	brokerAddrs := []string{"localhost:11111", "localhost:11112", "localhost:11113", "localhost:11114"}

	publisher := NewPublisher()

	publisher.StartBrokerClients(brokerAddrs)

	time.Sleep(time.Second)

	publisher.Publish(&pb.Publication{
		PubType:       common.AB,
		PublisherID:   int64(*publisherID),
		PublicationID: 1,
		Topic:         1,
		Content:       []byte(time.Now().String()),
	})
	time.Sleep(time.Second)

	publisher.Publish(&pb.Publication{
		PubType:       common.BRB,
		PublisherID:   int64(*publisherID),
		PublicationID: 2,
		Topic:         2,
		Content:       []byte(time.Now().String()),
	})
	time.Sleep(time.Second)

	publisher.Publish(&pb.Publication{
		PubType:       common.AB,
		PublisherID:   int64(*publisherID),
		PublicationID: 3,
		Topic:         1,
		Content:       []byte(time.Now().String()),
	})
	time.Sleep(time.Second)
}
