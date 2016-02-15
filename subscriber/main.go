package main

import (
	"fmt"
)

// main starts a subscriber.
func main() {
	fmt.Printf("Subscriber started.\n")

	brokerAddrs := []string{"localhost:11111", "localhost:11112", "localhost:11113", "localhost:11114"}

	subscriber := NewSubscriber()
	subscriber.AddTopic(1)

	subscriber.StartBrokerClients(brokerAddrs)

	subscriber.ProcessPublications()

	for {
	}
}
