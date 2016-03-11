package main

import (
	"fmt"

	//"github.com/ericnorway/arbitraryFailures/common"
	//pb "github.com/ericnorway/arbitraryFailures/proto"
	"github.com/ericnorway/arbitraryFailures/broker"
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
	b := broker.NewBroker(localID, brokerAddresses[localID], uint64(*alpha))

	// Add publisher information
	for i, key := range publisherKeys {
		id := uint64(i)
		b.AddPublisher(id, []byte(key))
	}

	// Add broker information
	for i, key := range brokerKeys {
		id := uint64(i)
		if id != localID {
			b.AddBroker(id, brokerAddresses[id], []byte(key))
		}
	}

	// Add subscriber information
	for i, key := range subscriberKeys {
		id := uint64(i)
		b.AddSubscriber(id, []byte(key))
	}

	// Add the chain path
	b.AddChainPath(chain, rChain)

	// Start the broker
	b.StartBroker()
}
