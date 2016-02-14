package main

import (
	"fmt"
)

func main() {
	fmt.Printf("Subscriber started.\n")

	brokerAddrs := []string{"localhost:11111", "localhost:11112", "localhost:11113", "localhost:11114"}

	subscriber := NewSubscriber()
	subscriber.AddTopic(1)

	subscriber.startBrokerClients(brokerAddrs)

	subscriber.processPublications()

	for {
	}
}
