package broker

import (
	//"fmt"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// handleBrbPublish handles Bracha's Reliable Broadcast publish requests.
// It takes the request as input.
func (b Broker) handleBrbPublish(pub *pb.Publication) {
	// fmt.Printf("Handle BRB Publish Publication %v, Publisher %v, Broker %v.\n", pub.PublicationID, pub.PublisherID, pub.BrokerID)

	if b.echoesSent[pub.PublisherID] == nil {
		b.echoesSent[pub.PublisherID] = make(map[int64]bool)
	}

	// If this publication has not been echoed yet
	if b.echoesSent[pub.PublisherID][pub.PublicationID] == false {

		// Update broker ID
		pub.BrokerID = b.localID

		// "Send" the echo request to itself
		b.fromBrokerEchoCh <- *pub

		// Send the echo request to all other brokers
		b.remoteBrokersMutex.RLock()
		for _, remoteBroker := range b.remoteBrokers {
			if remoteBroker.toEchoCh != nil {
				remoteBroker.toEchoCh <- *pub
				if len(remoteBroker.toEchoCh) > b.toBrbChLen/2 {
					b.setBusy()
				}
			}
		}
		b.remoteBrokersMutex.RUnlock()

		// Mark this publication as echoed
		b.echoesSent[pub.PublisherID][pub.PublicationID] = true
		// fmt.Printf("Sent echoes for  publication %v by publisher %v\n", pub.PublicationID, pub.PublisherID)
	} else {
		// fmt.Printf("Already sent echoes for publication %v by publisher %v\n", pub.PublicationID, pub.PublisherID)
	}
}

// handleEcho handles echo requests from Bracha's Reliable Broadcast.
// It takes the request as input.
func (b Broker) handleEcho(pub *pb.Publication) {
	// fmt.Printf("Handle echo Publication %v, Publisher %v, Broker %v.\n", pub.PublicationID, pub.PublisherID, pub.BrokerID)

	// Make the map so not trying to access nil reference
	if b.echoesReceived[pub.PublisherID] == nil {
		b.echoesReceived[pub.PublisherID] = make(map[int64]map[uint64]string)
	}
	// Make the map so not trying to access nil reference
	if b.echoesReceived[pub.PublisherID][pub.PublicationID] == nil {
		b.echoesReceived[pub.PublisherID][pub.PublicationID] = make(map[uint64]string)
	}
	// Echo has not been received yet for this publisher ID, publication ID, broker ID
	if b.echoesReceived[pub.PublisherID][pub.PublicationID][pub.BrokerID] == "" {
		// So record it
		b.echoesReceived[pub.PublisherID][pub.PublicationID][pub.BrokerID] = common.GetInfo(pub)

		// Check if there is a quorum yet for this publisher ID and publication ID
		foundQuorum := b.checkEchoQuorum(pub.PublisherID, pub.PublicationID)

		if b.readiesSent[pub.PublisherID] == nil {
			b.readiesSent[pub.PublisherID] = make(map[int64]bool)
		}

		// If this publication has not been readied yet
		if foundQuorum && b.readiesSent[pub.PublisherID][pub.PublicationID] == false {

			// Update broker ID
			pub.BrokerID = b.localID

			// "Send" the ready request to itself
			b.fromBrokerReadyCh <- *pub

			// Send the ready to all other brokers
			b.remoteBrokersMutex.RLock()
			for _, remoteBroker := range b.remoteBrokers {
				if remoteBroker.toReadyCh != nil {
					remoteBroker.toReadyCh <- *pub
					if len(remoteBroker.toReadyCh) > b.toBrbChLen/2 {
						b.setBusy()
					}
				}
			}
			b.remoteBrokersMutex.RUnlock()

			// Send the publication to all subscribers
			b.subscribersMutex.RLock()
			for _, subscriber := range b.subscribers {
				// Only if they are interested in the topic
				if subscriber.toCh != nil && subscriber.topics[pub.TopicID] == true {
					subscriber.toCh <- *pub
					if len(subscriber.toCh) > b.toSubscriberChLen/2 {
						b.setBusy()
					}
				}
			}
			b.subscribersMutex.RUnlock()

			// Mark this publication as readied
			b.readiesSent[pub.PublisherID][pub.PublicationID] = true
			// fmt.Printf("handleEcho: Sent readies for publication %v by publisher %v.\n", pub.PublicationID, pub.PublisherID)

			// For performance testing, get the time of the last step for this broker
			b.incrementPublicationCount(pub)
		}
	}
}

// handleReady handles ready requests from Bracha's Reliable Broadcast.
// It takes the request as input.
func (b Broker) handleReady(pub *pb.Publication) {
	// fmt.Printf("Handle ready Publication %v, Publisher %v, Broker %v.\n", pub.PublicationID, pub.PublisherID, pub.BrokerID)

	// Make the map so not trying to access nil reference
	if b.readiesReceived[pub.PublisherID] == nil {
		b.readiesReceived[pub.PublisherID] = make(map[int64]map[uint64]string)
	}
	// Make the map so not trying to access nil reference
	if b.readiesReceived[pub.PublisherID][pub.PublicationID] == nil {
		b.readiesReceived[pub.PublisherID][pub.PublicationID] = make(map[uint64]string)
	}
	// Echo has not been received yet for this publisher ID, publication ID, broker ID
	if b.readiesReceived[pub.PublisherID][pub.PublicationID][pub.BrokerID] == "" {
		// So record it
		b.readiesReceived[pub.PublisherID][pub.PublicationID][pub.BrokerID] = common.GetInfo(pub)

		// Check if there is a quorum yet for this publisher ID and publication ID
		foundQuorum := b.checkReadyQuorum(pub.PublisherID, pub.PublicationID)

		if b.readiesSent[pub.PublisherID] == nil {
			b.readiesSent[pub.PublisherID] = make(map[int64]bool)
		}

		// If this publication has not been readied yet
		if foundQuorum && b.readiesSent[pub.PublisherID][pub.PublicationID] == false {

			// Update broker ID
			pub.BrokerID = b.localID

			// "Send" the ready request to itself, if not already from self
			if pub.BrokerID != pub.BrokerID {
				b.fromBrokerReadyCh <- *pub
			}

			// Send the ready to all other brokers
			b.remoteBrokersMutex.RLock()
			for _, remoteBroker := range b.remoteBrokers {
				if remoteBroker.toReadyCh != nil {
					remoteBroker.toReadyCh <- *pub
					if len(remoteBroker.toReadyCh) > b.toBrbChLen/2 {
						b.setBusy()
					}
				}
			}
			b.remoteBrokersMutex.RUnlock()

			// Send the publication to all subscribers
			b.subscribersMutex.RLock()
			for _, subscriber := range b.subscribers {
				// Only if they are interested in the topic
				if subscriber.toCh != nil && subscriber.topics[pub.TopicID] == true {
					subscriber.toCh <- *pub
					if len(subscriber.toCh) > b.toSubscriberChLen/2 {
						b.setBusy()
					}
				}
			}
			b.subscribersMutex.RUnlock()

			// Mark this publication as readied
			b.readiesSent[pub.PublisherID][pub.PublicationID] = true
			// fmt.Printf("handleReady: Sent readies for publication %v by publisher %v.\n", pub.PublicationID, pub.PublisherID)

			// For performance testing, get the time of the last step for this broker
			b.incrementPublicationCount(pub)
		}
	}
}

// checkEchoQuorum checks that a quorum has been received for a specific publisher and publication.
// It return true if a quorum has been found, false otherwise. It takes as input
// the publisher ID and publication ID.
func (b *Broker) checkEchoQuorum(publisherID uint64, publicationID int64) bool {

	// Just a temporary map to help with checking for a quorum. It keeps track of the number of each
	// publication value with this publisher ID and publication ID.
	countMap := make(map[string]uint64)

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
func (b *Broker) checkReadyQuorum(publisherID uint64, publicationID int64) bool {

	// Just a temporary map to help with checking for a quorum. It keeps track of the number of each
	// publication value with this publisher ID and publication ID.
	countMap := make(map[string]uint64)

	for _, readyContent := range b.readiesReceived[publisherID][publicationID] {
		countMap[readyContent] = countMap[readyContent] + 1
		if countMap[readyContent] >= b.readyQuorumSize {
			return true
		}
	}

	return false
}
