package publisher

import (
	"bytes"
	"encoding/binary"
	//"fmt"
	"time"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"
)

func (p *Publisher) addToHistory(pub *pb.Publication) {
	// Don't put the history publication in the history.
	if pub.PubType != common.BRB {
		p.history[pub.TopicID] = append(p.history[pub.TopicID], *pub)
		p.pubsSinceLastHistory[pub.TopicID]++
	}
}

func (p *Publisher) publishHistory(topicID uint64) {
	// Create the publication.
	pub := &pb.Publication{
		PubType:       common.BRB,
		PublisherID:   p.localID,
		PublicationID: p.historyID,
		TopicID:       topicID,
		Contents:      [][]byte{},
	}

	// For all the publications since the last history
	length := uint64(len(p.history[pub.TopicID]))
	for i := p.pubsSinceLastHistory[pub.TopicID]; i > 0; i-- {
		var buf bytes.Buffer
		publicationID := make([]byte, 8)

		// Write publication ID and contents to buffer
		binary.PutVarint(publicationID, p.history[pub.TopicID][length-i].PublicationID)
		buf.Write(publicationID)
		buf.Write(p.history[pub.TopicID][length-i].Contents[0])

		// Add to the contents
		pub.Contents = append(pub.Contents, buf.Bytes())
	}

	p.historyID--

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

	// Send the publication.
	sent := p.publish(pub)
	for sent != pb.PubResponse_OK {
		time.Sleep(100 * time.Microsecond)
		sent = p.publish(pub)
	}

	// Reset this
	p.pubsSinceLastHistory[topicID] = 0
}
