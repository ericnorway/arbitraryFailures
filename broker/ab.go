package main

import (
	// "fmt"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// handleAbPublish handles Authenticated Broadcast publish requests.
// It takes the request as input.
func (b Broker) handleAbPublish(req *pb.Publication) {
	// fmt.Printf("Handle AB Publish Publication %v, Publisher %v, Broker %v.\n", req.PublicationID, req.PublisherID, req.BrokerID)

	if b.forwardSent[req.PublisherID] == nil {
		b.forwardSent[req.PublisherID] = make(map[int64]bool)
	}

	// If this publication has not been forwarded yet
	if b.forwardSent[req.PublisherID][req.PublicationID] == false {

		// Make forward publication
		pub := &pb.Publication{
			PubType:       req.PubType,
			PublisherID:   req.PublisherID,
			PublicationID: req.PublicationID,
			Topic:         req.Topic,
			BrokerID:      int64(*brokerID),
			Content:       req.Content,
			MACs:          req.MACs,
		}

		// Forward the publication to all subscribers
		b.subscribersMutex.RLock()
		for i, subscriber := range b.subscribers {
			// Only if they are interested in the topic
			if subscriber.toCh != nil && b.subscribers[i].topics[pub.Topic] == true {
				subscriber.toCh <- pub
			}
		}
		b.subscribersMutex.RUnlock()

		// Mark this publication as sent
		b.forwardSent[req.PublisherID][req.PublicationID] = true
	} else {
		// fmt.Printf("Already forwarded publication %v by publisher %v\n", req.PublicationID, req.PublisherID)
	}
}
