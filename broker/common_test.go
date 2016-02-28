package broker

import (
	"bytes"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

var message1 = "Some data."
var message2 = "Some other data."
var message3 = "Some more data."
var message4 = "Even more data..."

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
	if a.Topic != b.Topic {
		return false
	}
	if a.BrokerID != b.BrokerID {
		return false
	}
	if !bytes.Equal(a.Content, b.Content) {
		return false
	}
	return true
}
