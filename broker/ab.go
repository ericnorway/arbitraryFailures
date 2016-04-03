package broker

import (
	// "fmt"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// handleAbPublish handles Authenticated Broadcast publish requests.
// It takes the request as input.
func (b Broker) handleAbPublish(pub *pb.Publication) {
	// fmt.Printf("Handle AB Publish Publication %v, Publisher %v, Broker %v.\n", pub.PublicationID, pub.PublisherID, pub.BrokerID)

	if b.forwardSent[pub.PublisherID] == nil {
		b.forwardSent[pub.PublisherID] = make(map[int64]bool)
	}

	// If this publication has not been forwarded yet
	if b.forwardSent[pub.PublisherID][pub.PublicationID] == false {

		// Update broker ID
		pub.BrokerID = b.localID

		// Forward the publication to all subscribers
		b.subscribersMutex.RLock()
		for i, subscriber := range b.subscribers {
			// Only if they are interested in the topic
			if subscriber.toCh != nil && b.subscribers[i].topics[pub.TopicID] == true {
				select {
				case subscriber.toCh <- *pub:
					if len(subscriber.toCh) > toChannelLength/2 {
						b.setBusy()
					}
				}
			}
		}
		b.subscribersMutex.RUnlock()

		// Mark this publication as sent
		b.forwardSent[pub.PublisherID][pub.PublicationID] = true

		// For performance testing, get the time of the last step for this broker
		select {
		case b.ToUserRecordCh <- true:
		default:
			// Use the default case just in case the user isn't reading from this channel
			// and the channel fills up.
		}
	} else {
		// fmt.Printf("Already forwarded publication %v by publisher %v\n", pub.PublicationID, pub.PublisherID)
	}
}
