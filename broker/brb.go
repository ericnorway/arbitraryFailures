package main

import (
	// "fmt"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// handleBrbPublish handles Bracha's Reliable Broadcast publish requests.
// It takes the request as input.
func (b Broker) handleBrbPublish(req *pb.Publication) {
	// fmt.Printf("%v\n", req)
	if b.echoesSent[req.PublisherID] == nil {
		b.echoesSent[req.PublisherID] = make(map[int64]bool)
	}

	// If this publication has not been echoed yet
	if b.echoesSent[req.PublisherID][req.PublicationID] == false {
		req.BrokerID = int64(*brokerID)

		// "Send" the echo request to itself
		b.handleEcho(req)

		// Send the echo request to all other brokers
		b.toBrokerEchoChs.RLock()
		for _, ch := range b.toBrokerEchoChs.chs {
			ch <- req
		}
		b.toBrokerEchoChs.RUnlock()

		// Mark this publication as echoed
		b.echoesSent[req.PublisherID][req.PublicationID] = true
		// fmt.Printf("Sent echoes for  publication %v by publisher %v\n", req.PublicationID, req.PublisherID)
	} else {
		// fmt.Printf("Already sent echoes for publication %v by publisher %v\n", req.PublicationID, req.PublisherID)
	}
}

// handleEcho handles echo requests from Bracha's Reliable Broadcast.
// It takes the request as input.
func (b Broker) handleEcho(req *pb.Publication) {
	// fmt.Printf("Handle echo Publication %v, Publisher %v, Broker %v.\n", req.PublicationID, req.PublisherID, req.BrokerID)

	// Make the map so not trying to access nil reference
	if b.echoesReceived[req.PublisherID] == nil {
		b.echoesReceived[req.PublisherID] = make(map[int64]map[int64][]byte)
	}
	// Make the map so not trying to access nil reference
	if b.echoesReceived[req.PublisherID][req.PublicationID] == nil {
		b.echoesReceived[req.PublisherID][req.PublicationID] = make(map[int64][]byte)
	}
	// Echo has not been received yet for this publisher ID, publication ID, broker ID
	if b.echoesReceived[req.PublisherID][req.PublicationID][req.BrokerID] == nil {
		// So record it
		b.echoesReceived[req.PublisherID][req.PublicationID][req.BrokerID] = req.Content
		// Check if there is a quorum yet for this publisher ID and publication ID
		foundQuorum := b.checkEchoQuorum(req.PublisherID, req.PublicationID)

		if !foundQuorum {
			return
		}
	}

	if b.readiesSent[req.PublisherID] == nil {
		b.readiesSent[req.PublisherID] = make(map[int64]bool)
	}

	// If this publication has not been echoed yet
	if b.readiesSent[req.PublisherID][req.PublicationID] == false {

		// Update the Broker ID
		req.BrokerID = int64(*brokerID)

		// "Send" the ready request to itself
		b.handleReady(req)

		// Send the ready to all other brokers
		b.toBrokerReadyChs.RLock()
		for _, ch := range b.toBrokerReadyChs.chs {
			ch <- req
		}
		b.toBrokerReadyChs.RUnlock()

		// Send the ready to all subscribers
		b.toSubscriberChs.RLock()
		for i, ch := range b.toSubscriberChs.chs {
			// Only if they are interested in the topic
			if b.topics[i][req.Topic] == true {
				ch <- req
			}
		}
		b.toSubscriberChs.RUnlock()

		// Mark this publication as readied
		b.readiesSent[req.PublisherID][req.PublicationID] = true
		// fmt.Printf("handleEcho: Sent readies for publication %v by publisher %v.\n", req.PublicationID, req.PublisherID)
	} else {
		// fmt.Printf("handleEcho: Already sent readies publication %v by publisher %v.\n", req.PublicationID, req.PublisherID)
	}
}

// handleReady handles ready requests from Bracha's Reliable Broadcast.
// It takes the request as input.
func (b Broker) handleReady(req *pb.Publication) {
	// fmt.Printf("Handle ready Publication %v, Publisher %v, Broker %v.\n", req.PublicationID, req.PublisherID, req.BrokerID)

	// Make the map so not trying to access nil reference
	if b.readiesReceived[req.PublisherID] == nil {
		b.readiesReceived[req.PublisherID] = make(map[int64]map[int64][]byte)
	}
	// Make the map so not trying to access nil reference
	if b.readiesReceived[req.PublisherID][req.PublicationID] == nil {
		b.readiesReceived[req.PublisherID][req.PublicationID] = make(map[int64][]byte)
	}
	// Echo has not been received yet for this publisher ID, publication ID, broker ID
	if b.readiesReceived[req.PublisherID][req.PublicationID][req.BrokerID] == nil {
		// So record it
		b.readiesReceived[req.PublisherID][req.PublicationID][req.BrokerID] = req.Content
		// Check if there is a quorum yet for this publisher ID and publication ID
		foundQuorum := b.checkReadyQuorum(req.PublisherID, req.PublicationID)

		if !foundQuorum {
			return
		}
	}

	if b.readiesSent[req.PublisherID] == nil {
		b.readiesSent[req.PublisherID] = make(map[int64]bool)
	}

	// If this publication has not been echoed yet
	if b.readiesSent[req.PublisherID][req.PublicationID] == false {

		// Update the Broker ID
		req.BrokerID = int64(*brokerID)

		// "Send" the ready request to itself
		b.handleReady(req)

		// Send the ready to all other brokers
		b.toBrokerReadyChs.RLock()
		for _, ch := range b.toBrokerReadyChs.chs {
			ch <- req
		}
		b.toBrokerReadyChs.RUnlock()

		// Send the ready to all subscribers
		b.toSubscriberChs.RLock()
		for i, ch := range b.toSubscriberChs.chs {
			// Only if they are interested in the topic
			if b.topics[i][req.Topic] == true {
				ch <- req
			}
		}
		b.toSubscriberChs.RUnlock()

		// Mark this publication as readied
		b.readiesSent[req.PublisherID][req.PublicationID] = true
		// fmt.Printf("handleReady: Sent readies for publication %v by publisher %v.\n", req.PublicationID, req.PublisherID)
	} else {
		// fmt.Printf("handleReady: Already sent readies publication %v by publisher %v.\n", req.PublicationID, req.PublisherID)
	}
}

// checkEchoQuorum checks that a quorum has been received for a specific publisher and publication.
// It return true if a quorum has been found, false otherwise. It takes as input
// the publisher ID and publication ID.
func (b *Broker) checkEchoQuorum(publisherID int64, publicationID int64) bool {

	// Just a temporary map to help with checking for a quorum. It keeps track of the number of each
	// publication value with this publisher ID and publication ID.
	countMap := make(map[string]int64)

	for _, publication := range b.echoesReceived[publisherID][publicationID] {
		pub := string(publication)
		countMap[pub] = countMap[pub] + 1
		if countMap[pub] >= b.echoQuorumSize {
			return true
		}
	}

	return false
}

// checkReadyQuorum checks that a quorum has been received for a specific publisher and publication.
// It return true if a quorum has been found, false otherwise. It takes as input
// the publisher ID and publication ID.
func (b *Broker) checkReadyQuorum(publisherID int64, publicationID int64) bool {

	// Just a temporary map to help with checking for a quorum. It keeps track of the number of each
	// publication value with this publisher ID and publication ID.
	countMap := make(map[string]int64)

	for _, publication := range b.readiesReceived[publisherID][publicationID] {
		pub := string(publication)
		countMap[pub] = countMap[pub] + 1
		if countMap[pub] >= b.readyQuorumSize {
			return true
		}
	}

	return false
}
