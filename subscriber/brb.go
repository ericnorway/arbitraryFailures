package subscriber

import (
	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// handleBrbPublication processes a Bracha's Reliable Broadcast publication.
// It takes as input a publication.
func (s *Subscriber) handleBrbPublication(pub *pb.Publication) bool {
	foundQuorum := false

	// Make the map so not trying to access nil reference
	if s.pubsReceived[pub.PublisherID] == nil {
		s.pubsReceived[pub.PublisherID] = make(map[int64]map[uint64]string)
	}
	// Make the map so not trying to access nil reference
	if s.pubsReceived[pub.PublisherID][pub.PublicationID] == nil {
		s.pubsReceived[pub.PublisherID][pub.PublicationID] = make(map[uint64]string)
	}
	// Publication has not been received yet for this publisher ID, publication ID, broker ID
	if s.pubsReceived[pub.PublisherID][pub.PublicationID][pub.BrokerID] == "" {
		// So record it
		s.pubsReceived[pub.PublisherID][pub.PublicationID][pub.BrokerID] = common.GetInfo(pub)
		// Check if there is a quorum yet for this publisher ID and publication ID
		foundQuorum = s.checkQuorum(pub.PublisherID, pub.PublicationID)

		if foundQuorum && len(pub.Contents) > 1 {
			s.handleHistoryPublication(pub)
		}
	}

	return foundQuorum
}
