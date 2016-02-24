package main

import (
	"fmt"
)

// main parses the command line arguments and starts the Broker
func main() {
	parsedCorrectly := ParseArgs()
	if !parsedCorrectly {
		return
	}

	err := ReadConfigFile(*configFile)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}

	// Create new broker
	broker := NewBroker(localID, brokerAddresses[localID])

	// Add publisher information
	for i, key := range publisherKeys {
		id := int64(i)
		broker.AddPublisher(id, []byte(key))
	}

	// Add broker information
	for i, key := range brokerKeys {
		id := int64(i)
		if id != localID {
			broker.AddBroker(id, brokerAddresses[id], []byte(key))
		}
	}

	// Add subscriber information
	for i, key := range subscriberKeys {
		id := int64(i)
		broker.AddSubscriber(id, []byte(key))
	}

	// Start the broker
	broker.StartBroker()
}
