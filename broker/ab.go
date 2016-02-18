package main

import(
	"fmt"
	
	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// handleAbPublish handles Authenticated Broadcast publish requests.
// It takes the request as input.
func (b Broker) handleAbPublish(req *pb.Publication) {
	if b.forwardSent[req.PublisherID] == nil {
		b.forwardSent[req.PublisherID] = make(map[int64]bool)
	}

	// If this publication has not been forwarded yet
	if b.forwardSent[req.PublisherID][req.PublicationID] == false {
		req.BrokerID = int64(*brokerID)

		// Forward the publication to all subscribers
		b.toSubscriberChs.RLock()
		for i, ch := range b.toSubscriberChs.chs {
			// Only if they are interested in the topic
			if b.topics[i][req.Topic] == true {
				ch<- req
			}
		}
		b.toSubscriberChs.RUnlock()

		// Mark this publication as sent
		b.forwardSent[req.PublisherID][req.PublicationID] = true
	} else {
		fmt.Printf("Already forwarded publication %v by publisher %v\n", req.PublicationID, req.PublisherID)
	}
}