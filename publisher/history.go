package publisher

import (
	"bytes"
	"encoding/binary"
	"time"

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
			// Don't put the history publication in the history.
			if pub.PubType != common.BRB {
				history[pub.TopicID] = append(history[pub.TopicID], pub)
				pubsSinceLastHistory[pub.TopicID]++
			}
		case info := <-p.historyRequestCh:
			if historyRequests[info.TopicID] == nil {
				historyRequests[info.TopicID] = make(map[uint64]bool)
			}

			// If a history Publication was recently sent, ignore this request.
			if pubsSinceLastHistory[info.TopicID] < 2 {
				continue
			}

			historyRequests[info.TopicID][info.BrokerID] = true

			// If quorum of AB or one of Chain
			if (info.PubType == common.AB && len(historyRequests[info.TopicID]) > len(p.brokers)/2) || info.PubType == common.Chain {
				// Create the publication.
				pub := &pb.Publication{
					PubType:       common.BRB,
					PublisherID:   p.localID,
					PublicationID: historyID,
					TopicID:       info.TopicID,
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

				// Let the user know about the history publication.
				// Mostly just used in performance testing.
				select {
				case p.ToUserRecordCh <- common.RecordTime{
					PublisherID:   pub.PublisherID,
					PublicationID: pub.PublicationID,
					Time:          time.Now().UnixNano(),
				}:
				default:
					// Use the default case just in case the user isn't reading from this channel
					// and the channel fills up.
				}

				// Publish the history
				for sent := false; sent == false; {
					sent = p.Publish(pub)
					time.Sleep(time.Millisecond)
				}

				// Reset these
				historyRequests[info.TopicID] = make(map[uint64]bool)
				pubsSinceLastHistory[info.TopicID] = 0
				p.blockTopic[info.TopicID] = false
				blockRequests[info.TopicID] = make(map[uint64]bool)
			}
		case info := <-p.blockCh:
			if blockRequests[info.TopicID] == nil {
				blockRequests[info.TopicID] = make(map[uint64]bool)
			}

			blockRequests[info.TopicID][info.BrokerID] = true

			// If quorum of AB or one of Chain
			if (info.PubType == common.AB && len(blockRequests[info.TopicID]) > len(p.brokers)/2) || info.PubType == common.Chain {
				// Don't allow any more publications from this topic.
				p.blockTopic[info.TopicID] = true
			}

			// Send a request for history.
			select {
			case p.historyRequestCh <- info:
			}
		}
	}
}
