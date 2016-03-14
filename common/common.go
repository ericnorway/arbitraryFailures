package common

import (
	"bytes"
	"encoding/binary"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

var Message1 = "Some data."
var Message2 = "Some other data."
var Message3 = "Some more data."
var Message4 = "Even more data..."

var Message1PubID1 = []byte{2, 0, 0, 0, 0, 0, 0, 0, 83, 111, 109, 101, 32, 100, 97, 116, 97, 46}
var Message3PubID2 = []byte{4, 0, 0, 0, 0, 0, 0, 0, 83, 111, 109, 101, 32, 109, 111, 114, 101, 32, 100, 97, 116, 97, 46}
var Message4PubID3 = []byte{6, 0, 0, 0, 0, 0, 0, 0, 69, 118, 101, 110, 32, 109, 111, 114, 101, 32, 100, 97, 116, 97, 46, 46, 46}

var ChainRange = 3

// Enumeration of the different algorithms used
const (
	AB    = iota // Authenticated Broadcast algorithm
	BRB          // Bracha's Reliable Broadcast algorithm
	Chain        // Chain algorithm
)

type RecordTime struct {
	PublisherID   uint64
	PublicationID int64
	Time          int64
}

// GetInfo gets important info to verify from the publication (content and topic).
// It returns a string containing the information. It takes as input the publication.
func GetInfo(pub *pb.Publication) string {
	var buf bytes.Buffer
	topicBytes := make([]byte, 8)

	binary.PutUvarint(topicBytes, pub.TopicID)
	buf.Write(topicBytes)
	for i := range pub.Contents {
		buf.Write(pub.Contents[i])
	}

	return buf.String()
}

// Equals compares two publications to see if they are equal.
// It returns true if they are equal. False otherwise.
// It takes as input two publications.
func Equals(a pb.Publication, b pb.Publication) bool {
	if a.PubType != b.PubType {
		return false
	}
	if a.PublisherID != b.PublisherID {
		return false
	}
	if a.PublicationID != b.PublicationID {
		return false
	}
	if a.TopicID != b.TopicID {
		return false
	}
	if a.BrokerID != b.BrokerID {
		return false
	}
	for i := range a.Contents {
		if !bytes.Equal(a.Contents[i], b.Contents[i]) {
			return false
		}
	}
	// Currently this is not checking each individual Chain MAC, only the length of ChainMACs slice.
	if len(a.ChainMACs) != len(b.ChainMACs) {
		return false
	}
	return true
}
