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

	err := ReadConfigFile(*configFile)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}

	subscriber := NewSubscriber(localID)

	// Add topics this subscriber is interested in.
	for _, topic := range topics {
		subscriber.AddTopic(topic)
	}

	// Add broker information
	for i, key := range brokerKeys {
		id := int64(i)
		subscriber.AddBroker(id, brokerAddresses[id], []byte(key))
	}

	go subscriber.Start()

	for {
		select {
		case pub := <-subscriber.ToUser:
			fmt.Printf("Got publication %v from publisher %v.\n", pub.PublicationID, pub.PublisherID)
		}
	}
}
