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

	publisher := NewPublisher()
	
	publisher.AddBroker(1, "localhost:11111", []byte("12345"))
	publisher.AddBroker(2, "localhost:11112", []byte("12345"))
	publisher.AddBroker(3, "localhost:11113", []byte("12345"))
	publisher.AddBroker(4, "localhost:11114", []byte("12345"))

	publisher.StartBrokerClients()

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
