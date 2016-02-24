package main

import (
	// "fmt"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// handleAbPublish handles Authenticated Broadcast publish requests.
// It takes the request as input.
func (b Broker) handleAbPublish(pub *pb.Publication) {
	// fmt.Printf("Handle AB Publish Publication %v, Publisher %v, Broker %v.\n", req.PublicationID, req.PublisherID, req.BrokerID)

	if b.forwardSent[pub.PublisherID] == nil {
		b.forwardSent[pub.PublisherID] = make(map[int64]bool)
	}

	// If this publication has not been forwarded yet
	if b.forwardSent[pub.PublisherID][pub.PublicationID] == false {
		
		// Make echo publication
		tempPub := &pb.Publication{
			PubType:       pub.PubType,
			PublisherID:   pub.PublisherID,
			PublicationID: pub.PublicationID,
			Topic:         pub.Topic,
			BrokerID:      b.localID,
			Content:       pub.Content,
			MACs:          pub.MACs,
		}

		// Forward the publication to all subscribers
		b.subscribersMutex.RLock()
		for i, subscriber := range b.subscribers {
			// Only if they are interested in the topic
			if subscriber.toCh != nil && b.subscribers[i].topics[pub.Topic] == true {	
				subscriber.toCh <- *tempPub
			}
		}
		b.subscribersMutex.RUnlock()

		// Mark this publication as sent
		b.forwardSent[pub.PublisherID][pub.PublicationID] = true
	} else {
		// fmt.Printf("Already forwarded publication %v by publisher %v\n", pub.PublicationID, pub.PublisherID)
	}
}
