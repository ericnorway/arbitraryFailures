package common

import (
	"bytes"
	"encoding/binary"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// Enumeration of the different algorithms used
const (
	AB    = iota // Authenticated Broadcast algorithm
	BRB          // Bracha's Reliable Broadcast algorithm
	Chain        // Chain algorithm
)

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
	return true
}
