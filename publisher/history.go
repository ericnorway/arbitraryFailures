package publisher

import (
	"bytes"
	"encoding/binary"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// historyHandler keeps a hisotry of the publications and sends alpha values when
// a quorum of alpha requests is reached.
func (p *Publisher) historyHandler() {
	// The key is TopicID
	pubsSinceLastHistory := make(map[uint64]uint64)
	// The first key is TopicID. The second key is BrokerID.
	historyRequests := make(map[uint64]map[uint64]bool)
	// The key in the TopicID
	history := make(map[uint64][]pb.Publication)
	// The Publication ID for the history publications. Use negative numbers.
	historyID := int64(-1)

	// The first key is TopicID. The second key is BrokerID
	blockRequests := make(map[uint64]map[uint64]bool)

	for {
		select {
		case pub := <-p.addToHistoryCh:
			history[pub.TopicID] = append(history[pub.TopicID], pub)
			pubsSinceLastHistory[pub.TopicID]++
		case pair := <-p.historyRequestCh:
			if historyRequests[pair.TopicID] == nil {
				historyRequests[pair.TopicID] = make(map[uint64]bool)
			}

			// If a history Publication was recently sent, ignore this request.
			if pubsSinceLastHistory[pair.TopicID] < 2 {
				continue
			}

			historyRequests[pair.TopicID][pair.BrokerID] = true

			if len(historyRequests[pair.TopicID]) > len(p.brokers)/2 {
				// Create the publication.
				pub := &pb.Publication{
					PubType:       common.BRB,
					PublisherID:   p.localID,
					PublicationID: historyID,
					TopicID:       pair.TopicID,
					Contents:      [][]byte{},
				}

				// For all the publications since the last history
				length := uint64(len(history[pub.TopicID]))
				for i := pubsSinceLastHistory[pub.TopicID]; i > 0; i-- {
					var buf bytes.Buffer
					publicationID := make([]byte, 8)

					// Write publication ID and contents to buffer
					binary.PutVarint(publicationID, history[pub.TopicID][length-i].PublicationID)
					buf.Write(publicationID)
					buf.Write(history[pub.TopicID][length-i].Contents[0])

					// Add to the contents
					pub.Contents = append(pub.Contents, buf.Bytes())
				}

				historyID--

				// Send the history to all brokers
				p.brokersMutex.RLock()
				for _, broker := range p.brokers {
					if broker.toCh != nil {
						select {
						case broker.toCh <- *pub:
						}
					}
				}
				p.brokersMutex.RUnlock()

				// Reset these
				historyRequests[pair.TopicID] = make(map[uint64]bool)
				pubsSinceLastHistory[pub.TopicID] = 0
				p.blockTopic[pair.TopicID] = false
				blockRequests[pair.TopicID] = make(map[uint64]bool)
			}
		case pair := <-p.blockCh:
			if blockRequests[pair.TopicID] == nil {
				blockRequests[pair.TopicID] = make(map[uint64]bool)
			}

			blockRequests[pair.TopicID][pair.BrokerID] = true

			// If more than one broker is blocking
			if len(blockRequests[pair.TopicID]) > len(p.brokers)/2 {
				// Don't allow any more publications from this topic.
				p.blockTopic[pair.TopicID] = true
			}

			// Send a request for history.
			select {
			case p.historyRequestCh <- pair:
			}
		}
	}
}
