package main

import (
	"bytes"
	"encoding/binary"
	// "fmt"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// handleBrbPublish handles Bracha's Reliable Broadcast publish requests.
// It takes the request as input.
func (b Broker) handleBrbPublish(req *pb.Publication) {
	// fmt.Printf("Handle BRB Publish Publication %v, Publisher %v, Broker %v.\n", req.PublicationID, req.PublisherID, req.BrokerID)

	if b.echoesSent[req.PublisherID] == nil {
		b.echoesSent[req.PublisherID] = make(map[int64]bool)
	}

	// If this publication has not been echoed yet
	if b.echoesSent[req.PublisherID][req.PublicationID] == false {

		// Make echo publication
		pub := &pb.Publication{
			PubType:       req.PubType,
			PublisherID:   req.PublisherID,
			PublicationID: req.PublicationID,
			Topic:         req.Topic,
			BrokerID:      b.localID,
			Content:       req.Content,
			MACs:          req.MACs,
		}

		// "Send" the echo request to itself
		b.fromBrokerEchoCh <- pub

		// Send the echo request to all other brokers
		b.remoteBrokersMutex.RLock()
		for _, remoteBroker := range b.remoteBrokers {
			if remoteBroker.toEchoCh != nil {
				remoteBroker.toEchoCh <- pub
			}
		}
		b.remoteBrokersMutex.RUnlock()

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
		b.echoesReceived[req.PublisherID] = make(map[int64]map[int64]string)
	}
	// Make the map so not trying to access nil reference
	if b.echoesReceived[req.PublisherID][req.PublicationID] == nil {
		b.echoesReceived[req.PublisherID][req.PublicationID] = make(map[int64]string)
	}
	// Echo has not been received yet for this publisher ID, publication ID, broker ID
	if b.echoesReceived[req.PublisherID][req.PublicationID][req.BrokerID] == "" {
		// So record it
		b.echoesReceived[req.PublisherID][req.PublicationID][req.BrokerID] = getInfo(req)

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

		// Make ready publication
		pub := &pb.Publication{
			PubType:       req.PubType,
			PublisherID:   req.PublisherID,
			PublicationID: req.PublicationID,
			Topic:         req.Topic,
			BrokerID:      b.localID,
			Content:       req.Content,
			MACs:          req.MACs,
		}

		// "Send" the ready request to itself
		b.fromBrokerReadyCh <- pub

		// Send the ready to all other brokers
		b.remoteBrokersMutex.RLock()
		for _, remoteBroker := range b.remoteBrokers {
			if remoteBroker.toReadyCh != nil {
				remoteBroker.toReadyCh <- pub
			}
		}
		b.remoteBrokersMutex.RUnlock()

		// Send the publication to all subscribers
		b.subscribersMutex.RLock()
		for i, subscriber := range b.subscribers {
			// Only if they are interested in the topic
			if subscriber.toCh != nil && b.subscribers[i].topics[pub.Topic] == true {
				subscriber.toCh <- pub
			}
		}
		b.subscribersMutex.RUnlock()

		// Mark this publication as readied
		b.readiesSent[req.PublisherID][req.PublicationID] = true
		// fmt.Printf("handleEcho: Sent readies for publication %v by publisher %v.\n", req.PublicationID, req.PublisherID)
		return
	}

	// fmt.Printf("handleEcho: Already sent readies publication %v by publisher %v.\n", req.PublicationID, req.PublisherID)
}

// handleReady handles ready requests from Bracha's Reliable Broadcast.
// It takes the request as input.
func (b Broker) handleReady(req *pb.Publication) {
	// fmt.Printf("Handle ready Publication %v, Publisher %v, Broker %v.\n", req.PublicationID, req.PublisherID, req.BrokerID)

	// Make the map so not trying to access nil reference
	if b.readiesReceived[req.PublisherID] == nil {
		b.readiesReceived[req.PublisherID] = make(map[int64]map[int64]string)
	}
	// Make the map so not trying to access nil reference
	if b.readiesReceived[req.PublisherID][req.PublicationID] == nil {
		b.readiesReceived[req.PublisherID][req.PublicationID] = make(map[int64]string)
	}
	// Echo has not been received yet for this publisher ID, publication ID, broker ID
	if b.readiesReceived[req.PublisherID][req.PublicationID][req.BrokerID] == "" {
		// So record it
		b.readiesReceived[req.PublisherID][req.PublicationID][req.BrokerID] = getInfo(req)

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

		// Make ready publication
		pub := &pb.Publication{
			PubType:       req.PubType,
			PublisherID:   req.PublisherID,
			PublicationID: req.PublicationID,
			Topic:         req.Topic,
			BrokerID:      b.localID,
			Content:       req.Content,
			MACs:          req.MACs,
		}

		// "Send" the ready request to itself, if not already from self
		if req.BrokerID != pub.BrokerID {
			b.fromBrokerReadyCh <- pub
		}

		// Send the ready to all other brokers
		b.remoteBrokersMutex.RLock()
		for _, remoteBroker := range b.remoteBrokers {
			if remoteBroker.toReadyCh != nil {
				remoteBroker.toReadyCh <- pub
			}
		}
		b.remoteBrokersMutex.RUnlock()

		// Send the publication to all subscribers
		b.subscribersMutex.RLock()
		for i, subscriber := range b.subscribers {
			// Only if they are interested in the topic
			if subscriber.toCh != nil && b.subscribers[i].topics[pub.Topic] == true {
				subscriber.toCh <- pub
			}
		}
		b.subscribersMutex.RUnlock()

		// Mark this publication as readied
		b.readiesSent[req.PublisherID][req.PublicationID] = true
		// fmt.Printf("handleReady: Sent readies for publication %v by publisher %v.\n", req.PublicationID, req.PublisherID)
		return
	}

	// fmt.Printf("handleReady: Already sent readies publication %v by publisher %v.\n", req.PublicationID, req.PublisherID)
}

// getInfo gets important info to verify from the publication (content and topic).
// It returns a string containing the information. It takes as input the publication.
func getInfo(pub *pb.Publication) string {
	var buf bytes.Buffer
	topicBytes := make([]byte, 8)

	buf.Write(pub.Content)
	binary.PutVarint(topicBytes, pub.Topic)
	buf.Write(topicBytes)

	return buf.String()
}

// checkEchoQuorum checks that a quorum has been received for a specific publisher and publication.
// It return true if a quorum has been found, false otherwise. It takes as input
// the publisher ID and publication ID.
func (b *Broker) checkEchoQuorum(publisherID int64, publicationID int64) bool {

	// Just a temporary map to help with checking for a quorum. It keeps track of the number of each
	// publication value with this publisher ID and publication ID.
	countMap := make(map[string]int64)

	for _, echoContent := range b.echoesReceived[publisherID][publicationID] {
		countMap[echoContent] = countMap[echoContent] + 1
		if countMap[echoContent] >= b.echoQuorumSize {
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

	for _, readyContent := range b.readiesReceived[publisherID][publicationID] {
		countMap[readyContent] = countMap[readyContent] + 1
		if countMap[readyContent] >= b.readyQuorumSize {
			return true
		}
	}

	return false
}
