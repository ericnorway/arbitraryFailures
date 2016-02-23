package main

import (
	"fmt"
)

// main starts a subscriber.
func main() {
	fmt.Printf("Subscriber started.\n")

	parsedCorrectly := ParseArgs()
	if !parsedCorrectly {
		return
	}

	subscriber := NewSubscriber(int64(*subscriberID))
	subscriber.AddTopic(1)
	subscriber.AddTopic(2)

	subscriber.AddBroker(1, "localhost:11111", []byte("12345"))
	subscriber.AddBroker(2, "localhost:11112", []byte("12345"))
	subscriber.AddBroker(3, "localhost:11113", []byte("12345"))
	subscriber.AddBroker(4, "localhost:11114", []byte("12345"))

	subscriber.StartBrokerClients()

	go subscriber.ProcessPublications()

	for {
		select {
		case pub := <-subscriber.ToUser:
			fmt.Printf("Got publication %v from publisher %v.\n", pub.PublicationID, pub.PublisherID)
		}
	}
}
