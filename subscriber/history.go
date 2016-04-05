package subscriber

import (
	"bytes"
	"encoding/binary"
	//"fmt"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// handleHistoryPublication processes a Bracha's Reliable Broadcast publication.
// It takes as input a publication.
func (s *Subscriber) handleHistoryPublication(pub *pb.Publication) {
	// Make the map so not trying to access nil reference
	if s.pubsLearned[pub.PublisherID] == nil {
		s.pubsLearned[pub.PublisherID] = make(map[int64]string)
	}

	// For each publication in the history
	for _, histPub := range pub.Contents {

		if len(histPub) < 8 {
			continue
		}

		buf := bytes.NewBuffer(histPub)

		publicationID, _ := binary.ReadVarint(buf)
		content := histPub[8:]

		// If a quorum has not been reached yet for this individual publication in the history.
		if s.pubsLearned[pub.PublisherID][publicationID] == "" {
			s.pubsLearned[pub.PublisherID][publicationID] = string(content)

			// Create the publication.
			pub := &pb.Publication{
				PubType:       pub.PubType,
				PublisherID:   pub.PublisherID,
				PublicationID: publicationID,
				TopicID:       pub.TopicID,
				BrokerID:      pub.BrokerID,
				Contents: [][]byte{
					content,
				},
			}

			//fmt.Printf("Learned publication %v from publisher %v from a history publication.\n", publicationID, pub.PublisherID)

			// Send it to the user.
			select {
			case s.ToUserPubCh <- *pub:
			}
		}
	}
}
