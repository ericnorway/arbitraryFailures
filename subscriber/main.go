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

	brokerAddrs := []string{"localhost:11111", "localhost:11112", "localhost:11113", "localhost:11114"}

	subscriber := NewSubscriber(int64(*subscriberID))
	subscriber.AddTopic(1)

	subscriber.StartBrokerClients(brokerAddrs)

	go subscriber.ProcessPublications()

	for {
		select {
		case pub := <-subscriber.ToUser:
			fmt.Printf("Got publication %v from publisher %v.\n", pub.PublicationID, pub.PublisherID)
		}
	}
}
