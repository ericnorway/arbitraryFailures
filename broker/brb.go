package main

import (
	"fmt"
	
	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// handleBrbPublish handles Bracha's Reliable Broadcast publish requests.
// It takes the request as input.
func (b Broker) handleBrbPublish(req *pb.Publication) {
	fmt.Printf("%v\n", req)
	if b.echoesSent[req.PublisherID] == nil {
		b.echoesSent[req.PublisherID] = make(map[int64]bool)
	}

	// If this publication has not been echoed yet
	if b.echoesSent[req.PublisherID][req.PublicationID] == false {
		req.BrokerID = int64(*brokerID)

		// Echo the publication to all brokers
		b.toBrokerEchoChs.RLock()
		for _, ch := range b.toBrokerEchoChs.chs {
			 ch<- req
		}
		b.toBrokerEchoChs.RUnlock()

		// Mark this publication as echoed
		b.echoesSent[req.PublisherID][req.PublicationID] = true
	} else {
		fmt.Printf("Already echoed publication %v by publisher %v\n", req.PublicationID, req.PublisherID)
	}
}

// handleEcho handles echo requests from Bracha's Reliable Broadcast.
// It takes the request as input.
func (b Broker) handleEcho(pub *pb.Publication) {
	fmt.Printf("Handle echo Publication %v, Publisher %v, Broker %v.\n", pub.PublicationID, pub.PublisherID, pub.BrokerID)
	
	// Make the map so not trying to access nil reference
	if b.echoesReceived[pub.PublisherID] == nil {
		b.echoesReceived[pub.PublisherID] = make(map[int64]map[int64][]byte)
	}
	// Make the map so not trying to access nil reference
	if b.echoesReceived[pub.PublisherID][pub.PublicationID] == nil {
		b.echoesReceived[pub.PublisherID][pub.PublicationID] = make(map[int64][]byte)
	}
	// Echo has not been received yet for this publisher ID, publication ID, broker ID
	if b.echoesReceived[pub.PublisherID][pub.PublicationID][pub.BrokerID] == nil {
		// So record it
		b.echoesReceived[pub.PublisherID][pub.PublicationID][pub.BrokerID] = pub.Content
		// Check if there is a quorum yet for this publisher ID and publication ID
		foundQuorum := b.checkEchoQuorum(pub.PublisherID, pub.PublicationID, 3)
		
		if !foundQuorum {
			return
		}
	}
	
	// Update the Broker ID
	pub.BrokerID = int64(*brokerID)
	
	// Echo the publication to all brokers
	b.toBrokerEchoChs.RLock()
	for _, ch := range b.toBrokerReadyChs.chs {
		ch<- pub
	}
	b.toBrokerEchoChs.RUnlock()
}

// handleReady handles ready requests from Bracha's Reliable Broadcast.
// It takes the request as input.
func (b Broker) handleReady(pub *pb.Publication) {
	fmt.Printf("TODO: Handle ready Publication %v, Publisher %v, Broker %v.\n", pub.PublicationID, pub.PublisherID, pub.BrokerID)
}

// checkEchoQuorum checks that a quorum has been received for a specific publisher and publication.
// It return true if a quorum has been found, false otherwise. It takes as input
// the publisher ID, publication ID, and quorum size.
func (b *Broker) checkEchoQuorum(publisherID int64, publicationID int64, quorumSize int) bool {
	// It's nil, so nothing to check.
	if b.echoesReceived[publisherID] == nil {
		b.echoesReceived[publisherID] = make(map[int64]map[int64][]byte)
		return false
	}
	// It's nil, so nothing to check.
	if b.echoesReceived[publisherID][publicationID] == nil {
		b.echoesReceived[publisherID][publicationID] = make(map[int64][]byte)
		return false
	}

	// Make the map so not trying to access nil reference
	if b.echoesSent[publisherID] == nil {
		b.echoesSent[publisherID] = make(map[int64]bool)
	}
	// If already echoed this publication
	if b.echoesSent[publisherID][publicationID] != true {
		// fmt.Printf("Already echoed publication %v from publisher %v.\n", publicationID, publisherID)
		return false
	}

	// Just a temporary map to help with checking for a quorum. It keeps track of the number of each
	// publication value with this publisher ID and publication ID.
	countMap := make(map[string]int)

	for _, publication := range b.echoesReceived[publisherID][publicationID] {
		pub := string(publication)
		countMap[pub] = countMap[pub] + 1
		if countMap[pub] >= quorumSize {
			b.echoesSent[publisherID][publicationID] = true
			return true
		}
	}

	return false
}